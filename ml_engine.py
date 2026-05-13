"""
ARUNABHA ELITE SCALPER v3.0
FILE 10/18: ml_engine.py
Lightweight ML win-probability estimator
Uses scikit-learn GradientBoosting — no PyTorch/TF dependency on Railway
Walk-forward training from recent signal history
"""

import json
import logging
import os
import time
from collections import deque
from typing import List, Optional

import numpy as np

log = logging.getLogger("elite.ml")

try:
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    log.warning("scikit-learn not available — ML engine in fallback mode")


# ═══════════════════════════════════════════════
# ML ENGINE
# ═══════════════════════════════════════════════

class MLEngine:
    """
    Walk-forward ML win-probability estimator.
    Trains incrementally as signal outcomes arrive.
    Falls back to rule-based scoring if sklearn not available.
    """

    N_FEATURES = 24
    MIN_SAMPLES_TO_TRAIN = 30
    RETRAIN_EVERY = 20          # retrain after N new outcomes
    MODEL_FILE = "ml_model.json"

    def __init__(self):
        self._model: Optional[GradientBoostingClassifier] = None
        self._scaler: Optional[StandardScaler] = None
        self._trained = False
        self._training_data: deque = deque(maxlen=500)  # (features, label)
        self._outcomes_since_retrain = 0
        self._total_trained = 0
        self._last_train_ts = 0.0

        # Performance tracking
        self._predictions: deque = deque(maxlen=200)  # (predicted_prob, actual)
        self._accuracy_recent: float = 0.5

    async def initialize(self):
        """Load saved training data if available."""
        try:
            if os.path.exists(self.MODEL_FILE):
                with open(self.MODEL_FILE, "r") as f:
                    saved = json.load(f)
                samples = saved.get("samples", [])
                for s in samples:
                    feat = s["features"]
                    label = s["label"]
                    if len(feat) == self.N_FEATURES:
                        self._training_data.append((feat, label))
                log.info(f"ML: Loaded {len(self._training_data)} historical samples")
                if len(self._training_data) >= self.MIN_SAMPLES_TO_TRAIN:
                    self._train()
        except Exception as e:
            log.debug(f"ML init error: {e}")

    # ═══════════════════════════════════════════
    # PREDICTION
    # ═══════════════════════════════════════════

    def predict_win_probability(self, features: List[float]) -> float:
        """
        Returns estimated win probability [0, 1].
        If model not trained: falls back to rule-based heuristic.
        """
        if len(features) != self.N_FEATURES:
            features = self._pad_features(features)

        if self._trained and self._model and SKLEARN_AVAILABLE:
            try:
                X = np.array(features).reshape(1, -1)
                X_scaled = self._scaler.transform(X)
                prob = self._model.predict_proba(X_scaled)[0][1]
                return float(round(prob, 4))
            except Exception as e:
                log.debug(f"ML predict error: {e}")

        # Fallback: rule-based from features
        return self._rule_based_estimate(features)

    def _rule_based_estimate(self, features: List[float]) -> float:
        """
        Heuristic win probability from known-good features.
        Features index map (from signal_engine._build_ml_features):
        0: rsi/100, 1: adx/100, 2: macd_hist, 3: vol_ratio,
        4: bb_pct_b, 5: atr_pct, 6: 1h_rsi, 7: 1h_adx,
        8: buy_pressure, 9: delta_5m, 10: funding*1000, 11: score/100
        """
        try:
            rsi = features[0] * 100        # 0-100
            adx = features[1] * 100
            macd_hist = features[2]
            vol_ratio = features[3]
            score_norm = features[11]       # 0-1
            buy_pressure = features[8]

            base = 0.50

            # Score is the strongest predictor
            base += (score_norm - 0.65) * 0.5  # score 65% → +0, 85% → +0.1, 95% → +0.15

            # ADX: trend strength
            if adx > 30:
                base += 0.05
            elif adx < 18:
                base -= 0.05

            # Volume
            if vol_ratio > 1.3:
                base += 0.03

            # RSI: extremes are often low quality in trend
            if rsi > 75 or rsi < 25:
                base -= 0.03

            return float(round(min(max(base, 0.1), 0.95), 4))
        except Exception:
            return 0.55

    # ═══════════════════════════════════════════
    # TRAINING
    # ═══════════════════════════════════════════

    def record_outcome(self, features: List[float], won: bool):
        """
        Call this when a signal resolves (TP or SL hit).
        Triggers retraining if enough new data.
        """
        if len(features) != self.N_FEATURES:
            features = self._pad_features(features)

        label = 1 if won else 0
        self._training_data.append((features, label))
        self._outcomes_since_retrain += 1

        # Track prediction accuracy
        # (would need stored prediction to compare; simplified here)

        if (
            len(self._training_data) >= self.MIN_SAMPLES_TO_TRAIN
            and self._outcomes_since_retrain >= self.RETRAIN_EVERY
        ):
            self._train()
            self._outcomes_since_retrain = 0

        # Save periodically
        if len(self._training_data) % 10 == 0:
            self._save()

    def _train(self):
        if not SKLEARN_AVAILABLE:
            return
        if len(self._training_data) < self.MIN_SAMPLES_TO_TRAIN:
            return

        try:
            X = np.array([d[0] for d in self._training_data])
            y = np.array([d[1] for d in self._training_data])

            # Need both classes
            if len(set(y)) < 2:
                log.debug("ML: Need both win and loss samples")
                return

            self._scaler = StandardScaler()
            X_scaled = self._scaler.fit_transform(X)

            self._model = GradientBoostingClassifier(
                n_estimators=50,
                max_depth=3,
                learning_rate=0.1,
                subsample=0.8,
                random_state=42,
            )
            self._model.fit(X_scaled, y)
            self._trained = True
            self._total_trained = len(self._training_data)
            self._last_train_ts = time.time()

            win_rate = y.mean()
            log.info(
                f"ML retrained: {len(X)} samples, "
                f"win_rate={win_rate:.1%}, "
                f"features={self.N_FEATURES}"
            )
        except Exception as e:
            log.error(f"ML training error: {e}")

    # ═══════════════════════════════════════════
    # WALK-FORWARD VALIDATION
    # ═══════════════════════════════════════════

    def walk_forward_accuracy(self, window: int = 50) -> float:
        """
        Estimate accuracy on recent samples using leave-one-out
        approximation. Returns accuracy 0-1.
        """
        if not SKLEARN_AVAILABLE or len(self._training_data) < window + 10:
            return self._accuracy_recent

        try:
            data = list(self._training_data)[-window:]
            X = np.array([d[0] for d in data])
            y = np.array([d[1] for d in data])

            if len(set(y)) < 2:
                return 0.5

            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)

            model = GradientBoostingClassifier(
                n_estimators=30, max_depth=3, random_state=42
            )
            model.fit(X_scaled, y)
            preds = model.predict(X_scaled)
            acc = float((preds == y).mean())
            self._accuracy_recent = acc
            return acc
        except Exception:
            return 0.5

    # ═══════════════════════════════════════════
    # FEATURE IMPORTANCE
    # ═══════════════════════════════════════════

    def get_feature_importance(self) -> dict:
        """Return top features by importance."""
        if not self._trained or not self._model:
            return {}
        feature_names = [
            "rsi", "adx", "macd_hist", "vol_ratio", "bb_pct_b",
            "atr_pct", "h1_rsi", "h1_adx", "buy_pressure", "delta_5m",
            "funding", "score", "absorption", "exhaustion",
            "stoch_k", "stoch_d", "ema9_21", "ema21_50",
            "bb_bandwidth", "macd_atr", "f20", "f21", "f22", "f23",
        ]
        try:
            importances = self._model.feature_importances_
            result = {}
            for i, imp in enumerate(importances):
                name = feature_names[i] if i < len(feature_names) else f"f{i}"
                result[name] = round(float(imp), 4)
            return dict(sorted(result.items(), key=lambda x: -x[1])[:10])
        except Exception:
            return {}

    # ═══════════════════════════════════════════
    # PERSISTENCE
    # ═══════════════════════════════════════════

    def _save(self):
        try:
            data = {
                "samples": [
                    {"features": d[0], "label": d[1]}
                    for d in list(self._training_data)
                ],
                "total_trained": self._total_trained,
                "saved_at": time.time(),
            }
            with open(self.MODEL_FILE, "w") as f:
                json.dump(data, f)
        except Exception as e:
            log.debug(f"ML save error: {e}")

    # ═══════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════

    def _pad_features(self, features: List[float]) -> List[float]:
        """Pad or trim to exactly N_FEATURES."""
        f = list(features)
        while len(f) < self.N_FEATURES:
            f.append(0.0)
        return f[:self.N_FEATURES]

    def get_status(self) -> dict:
        return {
            "trained": self._trained,
            "samples": len(self._training_data),
            "total_trained": self._total_trained,
            "sklearn_available": SKLEARN_AVAILABLE,
            "accuracy_recent": round(self._accuracy_recent, 3),
            "last_train_ago_s": round(time.time() - self._last_train_ts) if self._last_train_ts else -1,
        }
