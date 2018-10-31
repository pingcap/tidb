package sarama

import (
	"time"
)

type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

func (t *AbortedTransaction) decode(pd packetDecoder) (err error) {
	if t.ProducerID, err = pd.getInt64(); err != nil {
		return err
	}

	if t.FirstOffset, err = pd.getInt64(); err != nil {
		return err
	}

	return nil
}

func (t *AbortedTransaction) encode(pe packetEncoder) (err error) {
	pe.putInt64(t.ProducerID)
	pe.putInt64(t.FirstOffset)

	return nil
}

type FetchResponseBlock struct {
	Err                 KError
	HighWaterMarkOffset int64
	LastStableOffset    int64
	AbortedTransactions []*AbortedTransaction
	Records             *Records // deprecated: use FetchResponseBlock.Records
	RecordsSet          []*Records
	Partial             bool
}

func (b *FetchResponseBlock) decode(pd packetDecoder, version int16) (err error) {
	tmp, err := pd.getInt16()
	if err != nil {
		return err
	}
	b.Err = KError(tmp)

	b.HighWaterMarkOffset, err = pd.getInt64()
	if err != nil {
		return err
	}

	if version >= 4 {
		b.LastStableOffset, err = pd.getInt64()
		if err != nil {
			return err
		}

		numTransact, err := pd.getArrayLength()
		if err != nil {
			return err
		}

		if numTransact >= 0 {
			b.AbortedTransactions = make([]*AbortedTransaction, numTransact)
		}

		for i := 0; i < numTransact; i++ {
			transact := new(AbortedTransaction)
			if err = transact.decode(pd); err != nil {
				return err
			}
			b.AbortedTransactions[i] = transact
		}
	}

	recordsSize, err := pd.getInt32()
	if err != nil {
		return err
	}

	recordsDecoder, err := pd.getSubset(int(recordsSize))
	if err != nil {
		return err
	}

	b.RecordsSet = []*Records{}

	for recordsDecoder.remaining() > 0 {
		records := &Records{}
		if err := records.decode(recordsDecoder); err != nil {
			// If we have at least one decoded records, this is not an error
			if err == ErrInsufficientData {
				if len(b.RecordsSet) == 0 {
					b.Partial = true
				}
				break
			}
			return err
		}

		partial, err := records.isPartial()
		if err != nil {
			return err
		}

		n, err := records.numRecords()
		if err != nil {
			return err
		}

		if n > 0 || (partial && len(b.RecordsSet) == 0) {
			b.RecordsSet = append(b.RecordsSet, records)

			if b.Records == nil {
				b.Records = records
			}
		}

		overflow, err := records.isOverflow()
		if err != nil {
			return err
		}

		if partial || overflow {
			break
		}
	}

	return nil
}

func (b *FetchResponseBlock) numRecords() (int, error) {
	sum := 0

	for _, records := range b.RecordsSet {
		count, err := records.numRecords()
		if err != nil {
			return 0, err
		}

		sum += count
	}

	return sum, nil
}

func (b *FetchResponseBlock) isPartial() (bool, error) {
	if b.Partial {
		return true, nil
	}

	if len(b.RecordsSet) == 1 {
		return b.RecordsSet[0].isPartial()
	}

	return false, nil
}

func (b *FetchResponseBlock) encode(pe packetEncoder, version int16) (err error) {
	pe.putInt16(int16(b.Err))

	pe.putInt64(b.HighWaterMarkOffset)

	if version >= 4 {
		pe.putInt64(b.LastStableOffset)

		if err = pe.putArrayLength(len(b.AbortedTransactions)); err != nil {
			return err
		}
		for _, transact := range b.AbortedTransactions {
			if err = transact.encode(pe); err != nil {
				return err
			}
		}
	}

	pe.push(&lengthField{})
	for _, records := range b.RecordsSet {
		err = records.encode(pe)
		if err != nil {
			return err
		}
	}
	return pe.pop()
}

type FetchResponse struct {
	Blocks       map[string]map[int32]*FetchResponseBlock
	ThrottleTime time.Duration
	Version      int16 // v1 requires 0.9+, v2 requires 0.10+
}

func (r *FetchResponse) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version

	if r.Version >= 1 {
		throttle, err := pd.getInt32()
		if err != nil {
			return err
		}
		r.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}

	numTopics, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	r.Blocks = make(map[string]map[int32]*FetchResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.getString()
		if err != nil {
			return err
		}

		numBlocks, err := pd.getArrayLength()
		if err != nil {
			return err
		}

		r.Blocks[name] = make(map[int32]*FetchResponseBlock, numBlocks)

		for j := 0; j < numBlocks; j++ {
			id, err := pd.getInt32()
			if err != nil {
				return err
			}

			block := new(FetchResponseBlock)
			err = block.decode(pd, version)
			if err != nil {
				return err
			}
			r.Blocks[name][id] = block
		}
	}

	return nil
}

func (r *FetchResponse) encode(pe packetEncoder) (err error) {
	if r.Version >= 1 {
		pe.putInt32(int32(r.ThrottleTime / time.Millisecond))
	}

	err = pe.putArrayLength(len(r.Blocks))
	if err != nil {
		return err
	}

	for topic, partitions := range r.Blocks {
		err = pe.putString(topic)
		if err != nil {
			return err
		}

		err = pe.putArrayLength(len(partitions))
		if err != nil {
			return err
		}

		for id, block := range partitions {
			pe.putInt32(id)
			err = block.encode(pe, r.Version)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func (r *FetchResponse) key() int16 {
	return 1
}

func (r *FetchResponse) version() int16 {
	return r.Version
}

func (r *FetchResponse) requiredVersion() KafkaVersion {
	switch r.Version {
	case 1:
		return V0_9_0_0
	case 2:
		return V0_10_0_0
	case 3:
		return V0_10_1_0
	case 4:
		return V0_11_0_0
	default:
		return MinVersion
	}
}

func (r *FetchResponse) GetBlock(topic string, partition int32) *FetchResponseBlock {
	if r.Blocks == nil {
		return nil
	}

	if r.Blocks[topic] == nil {
		return nil
	}

	return r.Blocks[topic][partition]
}

func (r *FetchResponse) AddError(topic string, partition int32, err KError) {
	if r.Blocks == nil {
		r.Blocks = make(map[string]map[int32]*FetchResponseBlock)
	}
	partitions, ok := r.Blocks[topic]
	if !ok {
		partitions = make(map[int32]*FetchResponseBlock)
		r.Blocks[topic] = partitions
	}
	frb, ok := partitions[partition]
	if !ok {
		frb = new(FetchResponseBlock)
		partitions[partition] = frb
	}
	frb.Err = err
}

func (r *FetchResponse) getOrCreateBlock(topic string, partition int32) *FetchResponseBlock {
	if r.Blocks == nil {
		r.Blocks = make(map[string]map[int32]*FetchResponseBlock)
	}
	partitions, ok := r.Blocks[topic]
	if !ok {
		partitions = make(map[int32]*FetchResponseBlock)
		r.Blocks[topic] = partitions
	}
	frb, ok := partitions[partition]
	if !ok {
		frb = new(FetchResponseBlock)
		partitions[partition] = frb
	}

	return frb
}

func encodeKV(key, value Encoder) ([]byte, []byte) {
	var kb []byte
	var vb []byte
	if key != nil {
		kb, _ = key.Encode()
	}
	if value != nil {
		vb, _ = value.Encode()
	}

	return kb, vb
}

func (r *FetchResponse) AddMessage(topic string, partition int32, key, value Encoder, offset int64) {
	frb := r.getOrCreateBlock(topic, partition)
	kb, vb := encodeKV(key, value)
	msg := &Message{Key: kb, Value: vb}
	msgBlock := &MessageBlock{Msg: msg, Offset: offset}
	if len(frb.RecordsSet) == 0 {
		records := newLegacyRecords(&MessageSet{})
		frb.RecordsSet = []*Records{&records}
	}
	set := frb.RecordsSet[0].MsgSet
	set.Messages = append(set.Messages, msgBlock)
}

func (r *FetchResponse) AddRecord(topic string, partition int32, key, value Encoder, offset int64) {
	frb := r.getOrCreateBlock(topic, partition)
	kb, vb := encodeKV(key, value)
	rec := &Record{Key: kb, Value: vb, OffsetDelta: offset}
	if len(frb.RecordsSet) == 0 {
		records := newDefaultRecords(&RecordBatch{Version: 2})
		frb.RecordsSet = []*Records{&records}
	}
	batch := frb.RecordsSet[0].RecordBatch
	batch.addRecord(rec)
}

func (r *FetchResponse) SetLastOffsetDelta(topic string, partition int32, offset int32) {
	frb := r.getOrCreateBlock(topic, partition)
	if len(frb.RecordsSet) == 0 {
		records := newDefaultRecords(&RecordBatch{Version: 2})
		frb.RecordsSet = []*Records{&records}
	}
	batch := frb.RecordsSet[0].RecordBatch
	batch.LastOffsetDelta = offset
}

func (r *FetchResponse) SetLastStableOffset(topic string, partition int32, offset int64) {
	frb := r.getOrCreateBlock(topic, partition)
	frb.LastStableOffset = offset
}
