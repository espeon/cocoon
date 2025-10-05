package server

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/Azure/go-autorest/autorest/to"
	"github.com/haileyok/cocoon/internal/helpers"
	"github.com/haileyok/cocoon/models"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
)

func (s *Server) handleSyncGetBlob(e echo.Context) error {
	did := e.QueryParam("did")
	if did == "" {
		return helpers.InputError(e, nil)
	}

	cstr := e.QueryParam("cid")
	if cstr == "" {
		return helpers.InputError(e, nil)
	}

	c, err := cid.Parse(cstr)
	if err != nil {
		return helpers.InputError(e, nil)
	}

	urepo, err := s.getRepoActorByDid(did)
	if err != nil {
		s.logger.Error("could not find user for requested blob", "error", err)
		return helpers.InputError(e, nil)
	}

	status := urepo.Status()
	if status != nil {
		if *status == "deactivated" {
			return helpers.InputError(e, to.StringPtr("RepoDeactivated"))
		}
	}

	var blob models.Blob
	if err := s.db.Raw("SELECT * FROM blobs WHERE did = ? AND cid = ?", nil, did, c.Bytes()).Scan(&blob).Error; err != nil {
		s.logger.Error("error looking up blob", "error", err)
		return helpers.ServerError(e, nil)
	}

	// get # of parts for blob size calculation
	var partCount int64
	if err := s.db.Raw("SELECT COUNT(*) FROM blob_parts WHERE blob_id = ?", nil, blob.ID).Scan(&partCount).Error; err != nil {
		s.logger.Error("error counting blob parts", "error", err)
		return helpers.ServerError(e, nil)
	}

	if partCount == 0 {
		s.logger.Error("blob has no parts", "blob_id", blob.ID)
		return helpers.ServerError(e, nil)
	}

	// get last part to calculate total size
	var lastPart models.BlobPart
	if err := s.db.Raw("SELECT * FROM blob_parts WHERE blob_id = ? ORDER BY idx DESC LIMIT 1", nil, blob.ID).Scan(&lastPart).Error; err != nil {
		s.logger.Error("error getting last blob part", "error", err)
		return helpers.ServerError(e, nil)
	}

	totalSize := int64((partCount-1)*BlockSize) + int64(len(lastPart.Data))

	e.Response().Header().Set(echo.HeaderContentDisposition, "attachment; filename="+c.String())
	e.Response().Header().Set("Accept-Ranges", "bytes")

	// validate + parse range header
	rangeHeader := e.Request().Header.Get("Range")
	if rangeHeader == "" {
		return s.streamAllParts(e, blob.ID)
	}

	ranges, err := parseRange(rangeHeader, totalSize)
	if err != nil || len(ranges) == 0 {
		return s.streamAllParts(e, blob.ID)
	}

	// important ! for now we should only just handle single range requests
	if len(ranges) > 1 {
		return s.streamAllParts(e, blob.ID)
	}
	r := ranges[0]

	if r.start >= totalSize || r.start < 0 || r.end > totalSize || r.start > r.end {
		e.Response().Header().Set("Content-Range", fmt.Sprintf("bytes */%d", totalSize))
		return e.NoContent(http.StatusRequestedRangeNotSatisfiable)
	}

	e.Response().Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", r.start, r.end-1, totalSize))
	e.Response().Header().Set("Content-Length", strconv.FormatInt(r.end-r.start, 10))

	// calculate which parts we need to fetch
	startPart := int(r.start / BlockSize)
	endPart := int((r.end - 1) / BlockSize)

	// get just the parts we need
	var parts []models.BlobPart
	if err := s.db.Raw("SELECT * FROM blob_parts WHERE blob_id = ? AND idx >= ? AND idx <= ? ORDER BY idx",
		nil, blob.ID, startPart, endPart).Scan(&parts).Error; err != nil {
		s.logger.Error("error getting blob parts", "error", err)
		return helpers.ServerError(e, nil)
	}

	if len(parts) == 0 {
		s.logger.Error("no parts found for range", "blob_id", blob.ID, "startPart", startPart, "endPart", endPart)
		return helpers.ServerError(e, nil)
	}

	// build
	buf := new(bytes.Buffer)
	for i, p := range parts {
		if i == 0 {
			offsetInPart := int(r.start % BlockSize)
			buf.Write(p.Data[offsetInPart:])
		} else if i == len(parts)-1 {
			offsetInPart := min(int((r.end-1)%BlockSize)+1, len(p.Data))
			buf.Write(p.Data[:offsetInPart])
		} else {
			buf.Write(p.Data)
		}
	}

	return e.Stream(http.StatusPartialContent, "application/octet-stream", buf)
}

func (s *Server) streamAllParts(e echo.Context, blobID uint) error {
	var parts []models.BlobPart
	if err := s.db.Raw("SELECT * FROM blob_parts WHERE blob_id = ? ORDER BY idx", nil, blobID).Scan(&parts).Error; err != nil {
		s.logger.Error("error getting blob parts", "error", err)
		return helpers.ServerError(e, nil)
	}

	buf := new(bytes.Buffer)
	for _, p := range parts {
		buf.Write(p.Data)
	}

	return e.Stream(http.StatusOK, "application/octet-stream", buf)
}

type byteRange struct {
	start int64
	end   int64
}

// Parse a range header value
func parseRange(s string, size int64) ([]byteRange, error) {
	if !strings.HasPrefix(s, "bytes=") {
		return nil, fmt.Errorf("invalid range header")
	}

	var ranges []byteRange
	s = strings.TrimPrefix(s, "bytes=")

	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)

		if part == "" {
			continue
		}

		dashIdx := strings.Index(part, "-")
		if dashIdx < 0 {
			return nil, fmt.Errorf("invalid range format")
		}

		startStr := part[:dashIdx]
		endStr := part[dashIdx+1:]

		var start, end int64

		if startStr == "" {
			// get the suffix
			if endStr == "" {
				return nil, fmt.Errorf("invalid range format")
			}
			suffix, err := strconv.ParseInt(endStr, 10, 64)
			if err != nil {
				return nil, err
			}
			if suffix > size {
				suffix = size
			}
			start = size - suffix
			end = size
		} else {
			var err error
			start, err = strconv.ParseInt(startStr, 10, 64)
			if err != nil {
				return nil, err
			}

			if endStr == "" {
				// open ended
				end = size
			} else {
				end, err = strconv.ParseInt(endStr, 10, 64)
				if err != nil {
					return nil, err
				}
				end++ // end is inclusive in the header, exclusive in our struct
			}
		}

		if start < 0 || start >= size {
			continue
		}

		if end > size {
			end = size
		}

		if start < end {
			ranges = append(ranges, byteRange{start: start, end: end})
		}
	}

	return ranges, nil
}
