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

func executeTSMCandidateCursorOutputs(locationsByKey map[string][]tsmCursorCandidate, tombstones []tsmTombstoneEntry, queryRange TimeRange, decodedOnly bool, descending bool) tsmCursorExecution {
	locations := map[string][]tsmCursorReadLocation{}
	for key, candidates := range locationsByKey {
		for _, candidate := range candidates {
			if decodedOnly && !candidate.decoded {
				continue
			}
			locations[key] = append(locations[key], newTSMCursorReadLocation("", candidate.index, candidate.entry, tombstones, queryRange, descending))
		}
	}
	return executeTSMCursorOutputs(locations, queryRange, descending)
}

func executeTSMFileStoreCursorOutputs(locationsByKey map[string][]tsmFileStoreLocation, queryRange TimeRange, decodedOnly bool, descending bool) tsmCursorExecution {
	locations := map[string][]tsmCursorReadLocation{}
	for key, fileLocations := range locationsByKey {
		for _, location := range fileLocations {
			if decodedOnly && !location.decoded {
				continue
			}
			locations[key] = append(locations[key], newTSMCursorReadLocation(location.path, location.index, location.entry, location.tombstones, queryRange, descending))
		}
	}
	return executeTSMCursorOutputs(locations, queryRange, descending)
}

func newTSMCursorReadLocation(path string, index int, entry tsmIndexEntry, tombstones []tsmTombstoneEntry, queryRange TimeRange, descending bool) tsmCursorReadLocation {
	seek := queryRange.Min
	readMin, readMax := int64(math.MinInt64), seek-1
	if descending {
		seek = queryRange.Max
		readMin, readMax = seek+1, int64(math.MaxInt64)
	}
	if (!descending && seek == math.MinInt64) || (descending && seek == math.MaxInt64) {
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

func executeTSMCursorOutputs(locationsByKey map[string][]tsmCursorReadLocation, queryRange TimeRange, descending bool) tsmCursorExecution {
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
		cursor := newTSMKeyCursor(locationsByKey[key], queryRange, descending)
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

type tsmKeyCursor struct {
	seeks      []tsmCursorReadLocation
	current    []*tsmCursorReadLocation
	pos        int
	descending bool
}

func newTSMKeyCursor(locations []tsmCursorReadLocation, queryRange TimeRange, descending bool) *tsmKeyCursor {
	seeks := append([]tsmCursorReadLocation(nil), locations...)
	sort.SliceStable(seeks, func(i, j int) bool {
		a, b := seeks[i], seeks[j]
		if rangesOverlap(a.entry.MinTime, a.entry.MaxTime, b.entry.MinTime, b.entry.MaxTime) {
			if a.path != b.path {
				return a.path < b.path
			}
			return a.index < b.index
		}
		if descending {
			return a.entry.MaxTime < b.entry.MaxTime
		}
		return a.entry.MinTime < b.entry.MinTime
	})

	cursor := &tsmKeyCursor{seeks: seeks, descending: descending}
	if descending {
		cursor.seek(queryRange.Max)
	} else {
		cursor.seek(queryRange.Min)
	}
	return cursor
}

func (c *tsmKeyCursor) seek(seek int64) {
	if c.descending {
		for i := len(c.seeks) - 1; i >= 0; i-- {
			location := &c.seeks[i]
			if seek > location.entry.MaxTime || tsmEntryContainsTime(location.entry, seek) {
				if len(c.current) == 0 {
					c.pos = i
				}
				c.current = append(c.current, location)
			}
		}
		return
	}
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

func (c *tsmKeyCursor) next() {
	if len(c.current) == 0 {
		return
	}
	if !c.current[0].read() {
		return
	}
	c.current = c.current[:0]
	if c.descending {
		c.nextDescending()
		return
	}
	c.nextAscending()
}

func (c *tsmKeyCursor) nextAscending() {
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

func (c *tsmKeyCursor) nextDescending() {
	for {
		c.pos--
		if c.pos < 0 {
			return
		}
		if !c.seeks[c.pos].read() {
			break
		}
	}

	c.current = append(c.current, &c.seeks[c.pos])
	for i := c.pos - 1; i >= 0; i-- {
		if c.seeks[i].read() {
			continue
		}
		c.current = append(c.current, &c.seeks[i])
	}
}

func (c *tsmKeyCursor) readBlock() ([]tsmPoint, bool) {
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
		if c.descending {
			return c.readDescendingWindow(values, minT, maxT, first)
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

func (c *tsmKeyCursor) readDescendingWindow(values []tsmPoint, minT, maxT int64, first *tsmCursorReadLocation) ([]tsmPoint, bool) {
	for i := 1; i < len(c.current); i++ {
		cur := c.current[i]
		if cur.entry.MaxTime > maxT && !cur.read() {
			maxT = cur.entry.MaxTime
		}
	}
	for i := 1; i < len(c.current); i++ {
		cur := c.current[i]
		if tsmEntryOverlapsTimeRange(cur.entry, minT, maxT) && !cur.read() {
			if cur.entry.MinTime < minT {
				minT = cur.entry.MinTime
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
			values = mergeTSMPoints(nextValues, values)
		}
		cur.markRead(minT, maxT)
	}
	first.markRead(minT, maxT)
	return values, true
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
	if l.path != "" {
		for i := range points {
			points[i].File = l.path
		}
	}
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
