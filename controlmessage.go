package claimcheck

import "strconv"

// Version is the current control-message envelope version.
const Version = "1"

// Metadata field suffixes, appended to Options.MetadataPrefix.
const (
	fieldVersion         = "v"
	fieldKey             = "key"
	fieldMessageCount    = "msg_count"
	fieldContentType     = "content_type"
	fieldContentEncoding = "content_encoding"
	fieldFileSize        = "file_size"
	fieldChecksum        = "checksum"
)

// ControlMessage is the standard claim-check envelope carried in a pubsub
// message's Metadata. It points at the offloaded blob and describes it.
type ControlMessage struct {
	Version         string
	Key             string
	MessageCount    int
	ContentType     string
	ContentEncoding string
	FileSize        int64
	Checksum        string
}

// ToMetadata renders the control message as a metadata map, prefixing each key
// with prefix.
func (c ControlMessage) ToMetadata(prefix string) map[string]string {
	return map[string]string{
		prefix + fieldVersion:         c.Version,
		prefix + fieldKey:             c.Key,
		prefix + fieldMessageCount:    strconv.Itoa(c.MessageCount),
		prefix + fieldContentType:     c.ContentType,
		prefix + fieldContentEncoding: c.ContentEncoding,
		prefix + fieldFileSize:        strconv.FormatInt(c.FileSize, 10),
		prefix + fieldChecksum:        c.Checksum,
	}
}

// HasControlMessageMetadata reports whether md carries a claim-check envelope
// under prefix — that is, whether the version field is present and non-empty —
// regardless of whether the rest of the envelope is well-formed. Use it together
// with ParseControlMessage to distinguish a genuine inline message (no envelope)
// from a corrupt or unsupported envelope (present but not parseable).
func HasControlMessageMetadata(md map[string]string, prefix string) bool {
	return md[prefix+fieldVersion] != ""
}

// ParseControlMessage extracts a ControlMessage from md using prefix. The bool
// is false when md carries no claim-check envelope (version field absent), when
// the version is not the supported Version, or when a numeric field is malformed.
// Use HasControlMessageMetadata to tell "no envelope" apart from "present but
// invalid".
func ParseControlMessage(md map[string]string, prefix string) (ControlMessage, bool) {
	v, ok := md[prefix+fieldVersion]
	if !ok || v != Version {
		return ControlMessage{}, false
	}
	count, err := strconv.Atoi(md[prefix+fieldMessageCount])
	if err != nil {
		return ControlMessage{}, false
	}
	size, err := strconv.ParseInt(md[prefix+fieldFileSize], 10, 64)
	if err != nil {
		return ControlMessage{}, false
	}
	return ControlMessage{
		Version:         v,
		Key:             md[prefix+fieldKey],
		MessageCount:    count,
		ContentType:     md[prefix+fieldContentType],
		ContentEncoding: md[prefix+fieldContentEncoding],
		FileSize:        size,
		Checksum:        md[prefix+fieldChecksum],
	}, true
}
