package main

import (
	"database/sql"
	"errors"
	"math"
	"net/http"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
	ride := &Ride{}
	if err := db.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 1`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// アクティブな車椅子の中から、配車位置に最も近い車椅子を選択
	type ChairWithLocation struct {
		ChairID   string `db:"chair_id"`
		Latitude  int    `db:"latitude"`
		Longitude int    `db:"longitude"`
	}

	chairs := []ChairWithLocation{}
	if err := db.SelectContext(ctx, &chairs, `
		SELECT 
			c.id as chair_id, 
			cl.latitude, 
			cl.longitude
		FROM chairs c
		INNER JOIN (
			SELECT chair_id, latitude, longitude
			FROM chair_locations cl1
			WHERE (chair_id, created_at) IN (
				SELECT chair_id, MAX(created_at)
				FROM chair_locations
				GROUP BY chair_id
			)
		) cl ON c.id = cl.chair_id
		WHERE c.is_active = TRUE
		AND NOT EXISTS (
			SELECT 1 FROM rides r
			INNER JOIN ride_statuses rs ON r.id = rs.ride_id
			WHERE r.chair_id = c.id
			AND rs.status NOT IN ('COMPLETED', 'CANCELED')
		)`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if len(chairs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// マンハッタン距離が最も近い車椅子を選択
	var nearestChair string
	minDistance := math.MaxInt32

	for _, chair := range chairs {
		distance := abs(chair.Latitude-ride.PickupLatitude) +
			abs(chair.Longitude-ride.PickupLongitude)
		if distance < minDistance {
			minDistance = distance
			nearestChair = chair.ChairID
		}
	}

	// 選択された車椅子とライドをマッチング
	if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", nearestChair, ride.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// 絶対値を計算するヘルパー関数
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
