package restapi

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/soumi/dfs/internal/gateway"
)

// putObject streams a request body into the cluster.
func (h *handlers) putObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "object key is required")
		return
	}

	res, err := h.engine.Put(r.Context(), bucket, key, r.Header.Get("Content-Type"), r.Body)
	if err != nil {
		h.writeEngineError(w, r, err)
		return
	}

	w.Header().Set("ETag", res.ETag)
	w.Header().Set("X-Dfs-Version-Id", res.VersionID)
	w.Header().Set("X-Dfs-Chunks", strconv.Itoa(int(res.Chunks)))
	// Surfaced so a client can see deduplication working rather than having to
	// infer it from disk usage.
	w.Header().Set("X-Dfs-Deduplicated-Chunks", strconv.Itoa(int(res.DeduplicatedNum)))

	writeJSON(w, http.StatusOK, map[string]any{
		"bucket":               bucket,
		"key":                  key,
		"size":                 res.Size,
		"etag":                 res.ETag,
		"version_id":           res.VersionID,
		"chunks":               res.Chunks,
		"deduplicated_chunks":  res.DeduplicatedNum,
		"bytes_uploaded":       res.BytesUploaded,
	})
}

// getObject streams an object, honouring HTTP Range.
func (h *handlers) getObject(w http.ResponseWriter, r *http.Request) {
	h.serveObject(w, r, true)
}

// headObject returns an object's metadata without its body.
func (h *handlers) headObject(w http.ResponseWriter, r *http.Request) {
	h.serveObject(w, r, false)
}

func (h *handlers) serveObject(w http.ResponseWriter, r *http.Request, withBody bool) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")

	// Resolve metadata first so Range can be validated against the real size
	// and rejected before any bytes are sent.
	info, err := h.engine.Get(r.Context(), bucket, key, 0, 0, nil)
	if err != nil {
		h.writeEngineError(w, r, err)
		return
	}

	offset, length, partial, err := parseRange(r.Header.Get("Range"), info.Size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", info.Size))
		writeError(w, http.StatusRequestedRangeNotSatisfiable, err.Error())
		return
	}

	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set("ETag", info.ETag)
	w.Header().Set("X-Dfs-Version-Id", info.VersionID)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))

	status := http.StatusOK
	if partial {
		w.Header().Set("Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, info.Size))
		status = http.StatusPartialContent
	}

	if !withBody {
		w.WriteHeader(status)
		return
	}

	w.WriteHeader(status)
	if _, err := h.engine.Get(r.Context(), bucket, key, offset, length, w); err != nil {
		// Headers are already sent, so the status cannot be corrected. Log it
		// and drop the connection — a truncated body with a 200 is the one
		// outcome worth avoiding, and closing mid-response signals the client.
		h.log.ErrorContext(r.Context(), "object read failed after headers were sent",
			"bucket", bucket, "key", key, "error", err)
		panic(http.ErrAbortHandler)
	}
}

func (h *handlers) deleteObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")

	deleted, err := h.engine.Delete(r.Context(), bucket, key)
	if err != nil {
		h.writeEngineError(w, r, err)
		return
	}
	if !deleted {
		// Idempotent: deleting something absent is a success, so retries after
		// an uncertain response do not produce spurious failures.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handlers) listObjects(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	q := r.URL.Query()

	maxKeys := 1000
	if v := q.Get("max-keys"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "max-keys must be a positive integer")
			return
		}
		maxKeys = n
	}

	res, err := h.engine.List(r.Context(), bucket,
		q.Get("prefix"), q.Get("delimiter"), q.Get("after"), int32(maxKeys))
	if err != nil {
		h.writeEngineError(w, r, err)
		return
	}

	objects := make([]map[string]any, len(res.Objects))
	for i, o := range res.Objects {
		objects[i] = map[string]any{
			"key":         o.Key,
			"size":        o.Size,
			"etag":        o.ETag,
			"modified_at": o.ModifiedAt,
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"bucket":          bucket,
		"objects":         objects,
		"common_prefixes": res.CommonPrefixes,
		"next_token":      res.NextToken,
		"is_truncated":    res.IsTruncated,
	})
}

func (h *handlers) createBucket(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	if !validBucketName(bucket) {
		writeError(w, http.StatusBadRequest,
			"bucket name must be 3-63 characters of lowercase letters, digits, hyphens or dots")
		return
	}

	if err := h.engine.CreateBucket(r.Context(), bucket); err != nil {
		h.writeEngineError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"bucket": bucket})
}

// parseRange interprets an HTTP Range header against a known object size.
//
// Only a single range is supported. Multi-range responses require multipart
// encoding, which no real client asks of an object store.
func parseRange(header string, size int64) (offset, length int64, partial bool, err error) {
	if header == "" {
		return 0, size, false, nil
	}
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false, errors.New("only byte ranges are supported")
	}

	spec := strings.TrimPrefix(header, "bytes=")
	if strings.Contains(spec, ",") {
		return 0, 0, false, errors.New("multiple ranges are not supported")
	}

	startStr, endStr, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, false, errors.New("malformed range")
	}

	switch {
	case startStr == "":
		// "bytes=-N": the final N bytes.
		n, convErr := strconv.ParseInt(endStr, 10, 64)
		if convErr != nil || n <= 0 {
			return 0, 0, false, errors.New("malformed suffix range")
		}
		if n > size {
			n = size
		}
		return size - n, n, true, nil

	case endStr == "":
		// "bytes=N-": from N to the end.
		start, convErr := strconv.ParseInt(startStr, 10, 64)
		if convErr != nil || start < 0 || start >= size {
			return 0, 0, false, fmt.Errorf("range start %s outside 0-%d", startStr, size-1)
		}
		return start, size - start, true, nil

	default:
		start, err1 := strconv.ParseInt(startStr, 10, 64)
		end, err2 := strconv.ParseInt(endStr, 10, 64)
		if err1 != nil || err2 != nil || start < 0 || start > end {
			return 0, 0, false, errors.New("malformed range")
		}
		if start >= size {
			return 0, 0, false, fmt.Errorf("range start %d outside 0-%d", start, size-1)
		}
		if end >= size {
			end = size - 1 // clamping is required by RFC 9110, not optional
		}
		return start, end - start + 1, true, nil
	}
}

// validBucketName applies S3's naming rules now, so names created through the
// native API in Phase 2 are still addressable through the S3 API in Phase 5.
func validBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	for i, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-' || c == '.':
			if i == 0 || i == len(name)-1 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func (h *handlers) writeEngineError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, gateway.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, gateway.ErrBucketExists):
		writeError(w, http.StatusConflict, "bucket already exists")
	case errors.Is(err, gateway.ErrInvalidRange):
		writeError(w, http.StatusRequestedRangeNotSatisfiable, err.Error())
	case errors.Is(err, gateway.ErrNoCapacity):
		writeError(w, http.StatusInsufficientStorage, "cluster is out of capacity")
	case errors.Is(err, gateway.ErrQuorum):
		// 503 rather than 500: the write did not happen, the cluster is
		// degraded, and retrying later is the right client behaviour.
		writeError(w, http.StatusServiceUnavailable, "write quorum not met; try again")
	default:
		h.log.ErrorContext(r.Context(), "request failed",
			"path", r.URL.Path, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}
