package ipq

type IPQ interface {
	Len() int

	SpaceLeft() bool

	// PushBack push element to the back of the queue
	// if k exists and move is true, element will be move to back
	// if k exists and update is true, Value of element will be replaced
	PushBack(k, v interface{}, move, update bool)

	Get(k interface{}) interface{}

	Del(k interface{})

	Range(f func(*Item) bool)

	DelInRange(f func(*Item) (bool, bool))

	Reset()

	Free()
}
