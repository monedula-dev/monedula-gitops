package mirror

import (
	"fmt"
	"strings"

	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
)

// Link config keys consulted by the mirror rules.
const (
	linkPrefixConfig      = "cluster.link.prefix"
	linkSyncIncludeConfig = "topic.config.sync.include"
)

// CheckPrefix validates a mirror's effective destination topic name against the
// link's cluster.link.prefix. An absent, null or empty prefix means none: the
// name must equal sourceTopicName. With a prefix, topicName must be declared
// explicitly and equal prefix+sourceTopicName. nil means the name is valid.
func CheckPrefix(l LinkInfo, topicName, sourceTopicName string, explicit bool) error {
	p := l.Configs[linkPrefixConfig]
	if p == "" {
		if topicName != sourceTopicName {
			return fmt.Errorf("topicName %q differs from sourceTopicName %q and link %q has no prefix",
				topicName, sourceTopicName, l.Link.Name)
		}
		return nil
	}
	want := p + sourceTopicName
	if !explicit || topicName != want {
		return fmt.Errorf("link %q has prefix %q; topicName must be %q", l.Link.Name, p, want)
	}
	return nil
}

// LinkActive reports whether the link is in a state that allows creating
// mirror topics on it (link_state ACTIVE).
func LinkActive(l LinkInfo) bool { return l.Link.State == "ACTIVE" }

// LinkErrorSuffix returns ": <link_error>" when REST reports a link error
// other than NO_ERROR, so messages about a link's state carry the reason;
// otherwise "".
func LinkErrorSuffix(l kafkarest.Link) string {
	if l.Error == "" || l.Error == "NO_ERROR" {
		return ""
	}
	return ": " + l.Error
}

// LinkManaged reports whether a topic config key is synced from the source by
// the link (topic.config.sync.include, exact entry or prefix entry ending in
// "*"). known is false when the link does not report the setting, or reports
// it empty (REST null), so ownership of the key cannot be determined.
func LinkManaged(l LinkInfo, key string) (managed bool, known bool) {
	v, ok := l.Configs[linkSyncIncludeConfig]
	if !ok || strings.TrimSpace(v) == "" {
		return false, false
	}
	for _, e := range strings.Split(v, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if e == key || (strings.HasSuffix(e, "*") && strings.HasPrefix(key, strings.TrimSuffix(e, "*"))) {
			return true, true
		}
	}
	return false, true
}
