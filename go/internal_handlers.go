package main

import (
	"database/sql"
	"errors"
	"net/http"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// 処理の流れ:
	// 1. マッチング待ちのライドを、移動距離が長い順に取得
	// 2. 各ライドに対して、最適な車椅子を選定
	// 3. 楽観ロックを用いて安全にマッチング

	ride := &Ride{}
	if err := db.GetContext(ctx, ride, `
		SELECT r.*, 
			ABS(r.destination_latitude - r.pickup_latitude) + 
			ABS(r.destination_longitude - r.pickup_longitude) as total_distance,
			r.version
		FROM rides r
		WHERE r.chair_id IS NULL
		ORDER BY total_distance DESC, created_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 利用可能な車椅子を、モデルの速度と現在位置を考慮して取得
	type ChairCandidate struct {
		ID        string `db:"id"`
		Model     string `db:"model"`
		Speed     int    `db:"speed"`
		Latitude  int    `db:"latitude"`
		Longitude int    `db:"longitude"`
		Distance  int    `db:"pickup_distance"`
	}

	chairs := []ChairCandidate{}
	if err := db.SelectContext(ctx, &chairs, `
		WITH latest_locations AS (
			SELECT DISTINCT ON (chair_id)
				chair_id,
				latitude,
				longitude
			FROM chair_locations
			ORDER BY chair_id, created_at DESC
		)
		SELECT 
			c.id,
			c.model,
			cm.speed,
			l.latitude,
			l.longitude,
			ABS(l.latitude - ?) + ABS(l.longitude - ?) as pickup_distance
		FROM chairs c
		INNER JOIN chair_models cm ON c.model = cm.name
		INNER JOIN latest_locations l ON c.id = l.chair_id
		WHERE c.is_active = TRUE
		AND NOT EXISTS (
			SELECT 1 FROM rides r
			INNER JOIN ride_statuses rs ON r.id = rs.ride_id
			WHERE r.chair_id = c.id
			AND rs.status NOT IN ('COMPLETED', 'CANCELED')
		)
		ORDER BY 
			cm.speed DESC,
			pickup_distance ASC
		LIMIT 10
	`, ride.PickupLatitude, ride.PickupLongitude); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if len(chairs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 楽観ロックを使用してマッチング
	result, err := db.ExecContext(ctx, `
		UPDATE rides 
		SET 
			chair_id = ?,
			version = version + 1
		WHERE id = ? 
		AND version = ? 
		AND chair_id IS NULL`,
		chairs[0].ID, ride.ID, ride.Version)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 楽観ロックが失敗した場合（他のプロセスが既にマッチングを行った）
	affected, err := result.RowsAffected()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if affected == 0 {
		w.WriteHeader(http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
