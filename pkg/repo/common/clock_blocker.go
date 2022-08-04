package common

import "time"

func TimeBlocker() {
    ticker := time.NewTicker(time.Second * 3)
    defer ticker.Stop()
    <-ticker.C
}
