package main

import (
	"log"
	"math"
	"math/rand"
)

func sample(cdf []float32) int {
	r := rand.Float32()
	bucket := 0
	for (bucket < len(cdf)) && (r > cdf[bucket]) {
		bucket++
	}
	if bucket >= len(cdf) {
		bucket = bucket - 1
	}
	return bucket
}

func sumArray(arr []float64) float64 {
	var sum float64
	for _, value := range arr {
		sum += value
	}
	return sum
}

func prepareCommandsDistribution(allQueries []string, queryRates []float64) (int, []float32) {
	totalRateSum := sumArray(queryRates)

	// probability density function
	if math.Abs(1.0-totalRateSum) > 0.01 {
		log.Fatalf("Total ratio should be 1.0 ( currently is %f )", totalRateSum)
	}

	pdf := make([]float32, len(allQueries))
	cdf := make([]float32, len(allQueries))
	for i := 0; i < len(queryRates); i++ {
		pdf[i] = float32(queryRates[i])
		cdf[i] = 0
	}
	// get cdf
	cdf[0] = pdf[0]
	for i := 1; i < len(queryRates); i++ {
		cdf[i] = cdf[i-1] + pdf[i]
	}
	return len(allQueries), cdf
}
