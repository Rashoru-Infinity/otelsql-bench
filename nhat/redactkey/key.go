package redactkey

type uniqueKey struct{}

func GetRedactHintKey() uniqueKey {
	return uniqueKey{}
}
