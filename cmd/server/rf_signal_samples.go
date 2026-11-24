package main

type rfScatterPoint struct {
	SNR  float64 `json:"snr"`
	RSSI float64 `json:"rssi"`
}

const (
	rfHasSNR uint8 = 1 << iota
	rfHasRSSI
	rfScatterPair
)

// Keep the original observation order without storing a second copy of
// the signal arrays. Legacy transmission-only samples affect histograms
// and medians, but never contribute scatter points.
type rfSignalSamples struct {
	snr, rssi []float64
	presence  []uint8
	pairs     int
}

func newRFSignalSamples(capacity int) rfSignalSamples {
	return rfSignalSamples{
		snr:      make([]float64, 0, capacity),
		rssi:     make([]float64, 0, capacity),
		presence: make([]uint8, 0, capacity),
	}
}

func (samples *rfSignalSamples) add(snr, rssi *float64, observed bool) {
	var presence uint8
	if snr != nil {
		samples.snr = append(samples.snr, *snr)
		presence |= rfHasSNR
	}
	if rssi != nil {
		samples.rssi = append(samples.rssi, *rssi)
		presence |= rfHasRSSI
	}
	if observed && presence == rfHasSNR|rfHasRSSI {
		presence |= rfScatterPair
		samples.pairs++
	}
	if presence != 0 {
		samples.presence = append(samples.presence, presence)
	}
}

func (samples *rfSignalSamples) scatter() []rfScatterPoint {
	step := max(1, samples.pairs/500)
	points := make([]rfScatterPoint, 0, (samples.pairs+step-1)/step)
	snrIndex, rssiIndex, pairIndex := 0, 0, 0
	for _, presence := range samples.presence {
		if presence&rfScatterPair != 0 {
			if pairIndex%step == 0 {
				points = append(points, rfScatterPoint{SNR: samples.snr[snrIndex], RSSI: samples.rssi[rssiIndex]})
			}
			pairIndex++
		}
		if presence&rfHasSNR != 0 {
			snrIndex++
		}
		if presence&rfHasRSSI != 0 {
			rssiIndex++
		}
	}
	return points
}
