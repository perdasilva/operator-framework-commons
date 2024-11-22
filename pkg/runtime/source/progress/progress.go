package progress

type EventType uint

const (
	// EventTypeNewTask will be fired on task start up
	EventTypeNewTask EventType = iota

	// EventTypeNewProgressed indicates that the task is currenctly in progress
	EventTypeNewProgressed

	// EventTypeDone indicates the task is done
	EventTypeDone

	// EventTypeSkipped indicates the task was skipped
	EventTypeSkipped

	// EventTypeExit indicates all tasks are done
	EventTypeExit = 4
)

type Event struct {
	EventType EventType

	// TaskID identifies the task (e.g. download, decompress, validate, etc.)
	TaskID string

	// SubjectID identifies the subject/file being acted on
	SubjectID string

	// Total size of the task (e.g. file size, etc.) -1 if unknown
	Size int64

	// How far the task has progressed. Will go from 0 to Size
	Offset int64

	// How far task has progressed since the last event
	OffsetUpdate int64
}

type Emitter struct {
	Event
	ProgressChan chan<- Event
}

func (b *Emitter) NewTask(taskID string) *Emitter {
	b.TaskID = taskID
	b.Size = -1
	b.SubjectID = ""
	b.OffsetUpdate = 0
	b.Offset = 0
	b.EventType = EventTypeNewTask
	return b
}

func (b *Emitter) WithSize(size int64) *Emitter {
	b.Size = size
	return b
}

func (b *Emitter) WithSubjectID(subjectID string) *Emitter {
	b.SubjectID = subjectID
	return b
}

func (b *Emitter) WithOffset(offset int64) *Emitter {
	b.Offset = offset
	return b
}

func (b *Emitter) WithOffsetUpdate(offset int64) *Emitter {
	b.OffsetUpdate = offset
	return b
}

func (b *Emitter) WithProgress(offset int64, offsetUpdate int64) *Emitter {
	return b.WithOffset(offset).WithOffsetUpdate(offsetUpdate)
}

func (b *Emitter) Skip() *Emitter {
	b.EventType = EventTypeSkipped
	return b
}

func (b *Emitter) Done() *Emitter {
	b.EventType = EventTypeDone
	return b
}

func (b *Emitter) Progress() *Emitter {
	b.EventType = EventTypeNewProgressed
	return b
}

func (b *Emitter) Emit() {
	if b.ProgressChan != nil {
		b.ProgressChan <- b.Event
	}
}
