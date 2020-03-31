package MqGet

var msg map[string]string
func SendMsg(m map[string]string) {
	msg = m
}

func GetMsg(string2 string) string {
	return msg[string2]
}

