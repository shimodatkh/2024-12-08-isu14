package main

import (
	"database/sql"
	"errors"
	"log/slog"
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

	matched := &Chair{}
	if err := db.GetContext(ctx, matched, `
		SELECT * FROM chairs 
		WHERE is_active = TRUE 
		AND can_match = TRUE
		ORDER BY RAND() 
		LIMIT 1`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			slog.Info("123 no matched chair")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		w.WriteHeader(http.StatusNoContent)

		return
	}
	// slogでマッチしたことをログに出す
	slog.Info("123 matched chair", "chair_id", matched.ID, "ride_id", ride.ID)

	result, err := db.ExecContext(ctx, `
		UPDATE rides r
		INNER JOIN chairs c ON c.id = ?
		SET r.chair_id = c.id,
			c.can_match = FALSE
		WHERE r.id = ?
		AND c.can_match = TRUE`, matched.ID, ride.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	affected, err := result.RowsAffected()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if affected == 0 {
		slog.Info("123 chair was already matched", "chair_id", matched.ID)
		w.WriteHeader(http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
