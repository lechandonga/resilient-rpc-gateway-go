package loadbalancer

import (
	"math/rand/v2"

	"github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"
)

// randomLB 按权重做加权随机选择；无状态，实例列表即全部输入。
type randomLB struct{}

func (randomLB) Name() string { return string(model.LBRandom) }

func (randomLB) Pick(candidates []model.Instance) (model.Instance, error) {
	if len(candidates) == 0 {
		return model.Instance{}, errEmptyCandidates
	}
	total := 0
	for _, in := range candidates {
		w := in.Weight
		if w <= 0 {
			w = 1
		}
		total += w
	}
	roll := rand.IntN(total)
	for _, in := range candidates {
		w := in.Weight
		if w <= 0 {
			w = 1
		}
		roll -= w
		if roll < 0 {
			return in, nil
		}
	}
	return candidates[len(candidates)-1], nil
}
