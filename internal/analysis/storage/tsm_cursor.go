package storage

import (
	"math"
	"sort"
)

type tsmCursorExecution struct {
	Points    map[tsmOutputPointKey]tsmPoint
	ReadCalls int
}

type tsmCursorReadLocation struct {
	path       string
	index      int
	entry      tsmIndexEntry
	tombstones []tsmTombstoneEntry
	readMin    int64
	readMax    int64
}

func executeTSMCandidateCursorOutputs(locationsByKey map[string][]tsmCursorCandidate, tombstones []tsmTombstoneEntry, queryRange TimeRange, decodedOnly bool) tsmCursorExecution {
	locations := map[string][]tsmCursorReadLocation{}
	for key, candidates := range locationsByKey {
		for _, candidate := range candidates {
			if decodedOnly && !candidate.decoded {
				continue
			}
			locations[key] = append(locations[key], newTSMCursorReadLocation("", candidate.index, candidate.entry, tombstones, queryRange.Min))
		}
	}
	return executeTSMCursorOutputs(locations, queryRange)
}

func executeTSMFileStoreCursorOutputs(locationsByKey map[string][]tsmFileStoreLocation, queryRange TimeRange, decodedOnly bool) tsmCursorExecution {
	locations := map[string][]tsmCursorReadLocation{}
	for key, fileLocations := range locationsByKey {
		for _, location := range fileLocations {
			if decodedOnly && !location.decoded {
				continue
			}
			locations[key] = append(locations[key], newTSMCursorReadLocation(location.path, location.index, location.entry, location.tombstones, queryRange.Min))
		}
	}
	return executeTSMCursorOutputs(locations, queryRange)
}

func newTSMCursorReadLocation(path string, index int, entry tsmIndexEntry, tombstones []tsmTombstoneEntry, seek int64) tsmCursorReadLocation {
	readMin, readMax := int64(math.MinInt64), seek-1
	if seek == math.MinInt64 {
		readMin, readMax = 1, 0
	}
	return tsmCursorReadLocation{
		path:       path,
		index:      index,
		entry:      entry,
		tombstones: tombstones,
		readMin:    readMin,
		readMax:    readMax,
	}
}

func executeTSMCursorOutputs(locationsByKey map[string][]tsmCursorReadLocation, queryRange TimeRange) tsmCursorExecution {
	execution := tsmCursorExecution{Points: map[tsmOutputPointKey]tsmPoint{}}
	if !queryRange.Set {
		return execution
	}

	keys := make([]string, 0, len(locationsByKey))
	for key := range locationsByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		cursor := newTSMAscendingCursor(locationsByKey[key], queryRange.Min)
		for {
			points, ok := cursor.readBlock()
			if !ok {
				break
			}
			execution.ReadCalls++
			points = includeTSMPoints(points, queryRange.Min, queryRange.Max)
			addTSMOutputPoints(execution.Points, key, points)
			cursor.next()
		}
	}
	return execution
}

type tsmAscendingCursor struct {
	seeks   []tsmCursorReadLocation
	current []*tsmCursorReadLocation
	pos     int
}

func newTSMAscendingCursor(locations []tsmCursorReadLocation, seek int64) *tsmAscendingCursor {
	seeks := append([]tsmCursorReadLocation(nil), locations...)
	sort.SliceStable(seeks, func(i, j int) bool {
		a, b := seeks[i], seeks[j]
		if rangesOverlap(a.entry.MinTime, a.entry.MaxTime, b.entry.MinTime, b.entry.MaxTime) {
			if a.path != b.path {
				return a.path < b.path
			}
			return a.index < b.index
		}
		return a.entry.MinTime < b.entry.MinTime
	})

	cursor := &tsmAscendingCursor{seeks: seeks}
	cursor.seek(seek)
	return cursor
}

func (c *tsmAscendingCursor) seek(seek int64) {
	for i := range c.seeks {
		location := &c.seeks[i]
		if seek < location.entry.MinTime || tsmEntryContainsTime(location.entry, seek) {
			if len(c.current) == 0 {
				c.pos = i
			}
			c.current = append(c.current, location)
		}
	}
}

func (c *tsmAscendingCursor) next() {
	if len(c.current) == 0 {
		return
	}
	if !c.current[0].read() {
		return
	}
	c.current = c.current[:0]
	for {
		c.pos++
		if c.pos >= len(c.seeks) {
			return
		}
		if !c.seeks[c.pos].read() {
			break
		}
	}

	c.current = append(c.current, &c.seeks[c.pos])
	for i := c.pos + 1; i < len(c.seeks); i++ {
		if c.seeks[i].read() {
			continue
		}
		c.current = append(c.current, &c.seeks[i])
	}
}

func (c *tsmAscendingCursor) readBlock() ([]tsmPoint, bool) {
	for {
		if len(c.current) == 0 {
			return nil, false
		}

		first := c.current[0]
		values := first.values()
		values = excludeTSMPoints(values, first.readMin, first.readMax)
		values = excludeTSMTombstones(values, first.entry.Key, first.tombstones)
		if len(values) == 0 && len(c.current) > 0 {
			c.current = c.current[1:]
			continue
		}

		if len(c.current) == 1 {
			if len(values) > 0 {
				first.markRead(pointsMinTime(values), pointsMaxTime(values))
			}
			return values, true
		}

		minT, maxT := first.readMin, first.readMax
		if len(values) > 0 {
			minT, maxT = pointsMinTime(values), pointsMaxTime(values)
		}
		for i := 1; i < len(c.current); i++ {
			cur := c.current[i]
			if cur.entry.MinTime < minT && !cur.read() {
				minT = cur.entry.MinTime
			}
		}
		for i := 1; i < len(c.current); i++ {
			cur := c.current[i]
			if tsmEntryOverlapsTimeRange(cur.entry, minT, maxT) && !cur.read() {
				if cur.entry.MaxTime > maxT {
					maxT = cur.entry.MaxTime
				}
				values = includeTSMPoints(values, minT, maxT)
				break
			}
		}
		for i := 1; i < len(c.current); i++ {
			cur := c.current[i]
			if !tsmEntryOverlapsTimeRange(cur.entry, minT, maxT) || cur.read() {
				cur.markRead(minT, maxT)
				continue
			}

			nextValues := cur.values()
			nextValues = excludeTSMTombstones(nextValues, cur.entry.Key, cur.tombstones)
			nextValues = excludeTSMPoints(nextValues, cur.readMin, cur.readMax)
			if len(nextValues) > 0 {
				nextValues = includeTSMPoints(nextValues, minT, maxT)
				values = mergeTSMPoints(values, nextValues)
			}
			cur.markRead(minT, maxT)
		}
		first.markRead(minT, maxT)
		return values, true
	}
}

func (l *tsmCursorReadLocation) read() bool {
	return l.readMin <= l.entry.MinTime && l.readMax >= l.entry.MaxTime
}

func (l *tsmCursorReadLocation) markRead(minTime, maxTime int64) {
	if minTime < l.readMin {
		l.readMin = minTime
	}
	if maxTime > l.readMax {
		l.readMax = maxTime
	}
}

func (l *tsmCursorReadLocation) values() []tsmPoint {
	if !l.entry.PointsAvailable {
		return nil
	}
	points := append([]tsmPoint(nil), l.entry.Points...)
	sort.SliceStable(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})
	return dedupeTSMPoints(points)
}

func tsmEntryContainsTime(entry tsmIndexEntry, timestamp int64) bool {
	return entry.MinTime <= timestamp && entry.MaxTime >= timestamp
}

func tsmEntryOverlapsTimeRange(entry tsmIndexEntry, minTime, maxTime int64) bool {
	return entry.MinTime <= maxTime && entry.MaxTime >= minTime
}

func excludeTSMTombstones(points []tsmPoint, key string, tombstones []tsmTombstoneEntry) []tsmPoint {
	for _, tombstone := range tombstones {
		if tombstone.Key == key {
			points = excludeTSMPoints(points, tombstone.Min, tombstone.Max)
		}
	}
	return points
}

func excludeTSMPoints(points []tsmPoint, minTime, maxTime int64) []tsmPoint {
	if len(points) == 0 || minTime > maxTime {
		return points
	}
	out := points[:0]
	for _, point := range points {
		if point.Timestamp >= minTime && point.Timestamp <= maxTime {
			continue
		}
		out = append(out, point)
	}
	return out
}

func includeTSMPoints(points []tsmPoint, minTime, maxTime int64) []tsmPoint {
	if len(points) == 0 || minTime > maxTime {
		return nil
	}
	out := points[:0]
	for _, point := range points {
		if point.Timestamp < minTime || point.Timestamp > maxTime {
			continue
		}
		out = append(out, point)
	}
	return out
}

func mergeTSMPoints(left, right []tsmPoint) []tsmPoint {
	left = dedupeTSMPoints(left)
	right = dedupeTSMPoints(right)
	if len(left) == 0 {
		return right
	}
	if len(right) == 0 {
		return left
	}
	if left[len(left)-1].Timestamp < right[0].Timestamp {
		return append(left, right...)
	}
	if right[len(right)-1].Timestamp < left[0].Timestamp {
		return append(right, left...)
	}

	out := make([]tsmPoint, 0, len(left)+len(right))
	for len(left) > 0 && len(right) > 0 {
		switch {
		case left[0].Timestamp < right[0].Timestamp:
			out = append(out, left[0])
			left = left[1:]
		case left[0].Timestamp == right[0].Timestamp:
			left = left[1:]
		default:
			out = append(out, right[0])
			right = right[1:]
		}
	}
	if len(left) > 0 {
		out = append(out, left...)
	}
	if len(right) > 0 {
		out = append(out, right...)
	}
	return out
}

func dedupeTSMPoints(points []tsmPoint) []tsmPoint {
	if len(points) <= 1 {
		return points
	}
	out := points[:1]
	for _, point := range points[1:] {
		if point.Timestamp == out[len(out)-1].Timestamp {
			continue
		}
		out = append(out, point)
	}
	return out
}

func pointsMinTime(points []tsmPoint) int64 {
	return points[0].Timestamp
}

func pointsMaxTime(points []tsmPoint) int64 {
	return points[len(points)-1].Timestamp
}
