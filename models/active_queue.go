package models

// ActiveQueue representa o estado de um consumidor de fila ativo.
type ActiveQueue struct {
	StopChan chan struct{}
}
