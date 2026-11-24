package main

import (
	"reflect"
	"strconv"
	"testing"
)

func TestRFSignalScatterUsesOriginalPairedOrder(t *testing.T) {
	for _, count := range []int{0, 499, 500, 501, 999, 1000, 1001} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			samples := newRFSignalSamples(0)
			expected := []rfScatterPoint{}
			step := max(1, count/500)
			for i := range count {
				snr, rssi := float64(i%31), float64(-90+i%20)
				samples.add(nil, nil, true)
				samples.add(&snr, nil, true)
				samples.add(nil, &rssi, true)
				samples.add(&rssi, &snr, false)
				samples.add(&snr, &rssi, true)
				if i%step == 0 {
					expected = append(expected, rfScatterPoint{SNR: snr, RSSI: rssi})
				}
			}
			if got := samples.scatter(); !reflect.DeepEqual(got, expected) {
				t.Fatalf("scatter count or order differs: got %d, want %d", len(got), len(expected))
			}
			if len(samples.snr) != 3*count || len(samples.rssi) != 3*count {
				t.Fatal("missing or legacy samples changed the scalar distributions")
			}
		})
	}
}
