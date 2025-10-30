# EWMA (Exponentially Weighted Moving Average) Explained

## The Formula

```python
ewma_alpha = 0.25  # The smoothing factor (α)

# First batch: initialize with actual value
ewma_rows_per_sec = rows_per_sec

# Subsequent batches: blend old and new
ewma_rows_per_sec = (alpha * rows_per_sec) + (1 - alpha) * ewma_rows_per_sec
                  = (0.25 * current)     + (0.75 * previous_ewma)
```

## Why Use EWMA?

### Problem: Simple Average is Too Reactive
```python
# Simple average of last 3 batches
[10000, 10000, 10000] → avg = 10,000 rows/s  ✓ Stable
[10000, 10000, 15000] → avg = 11,667 rows/s  ✓ Shows spike

# BUT: One spike causes big jump!
[15000] → avg jumps to 15,000 immediately  ❌ Too reactive
```

### Solution: EWMA Smooths Changes
```python
# EWMA with α=0.25
Previous: 10,000 rows/s
Current:  15,000 rows/s (spike!)

EWMA = 0.25 * 15,000 + 0.75 * 10,000
     = 3,750 + 7,500
     = 11,250 rows/s  ✓ Gradual change, not a jump!
```

## The Alpha Parameter (α = 0.25)

**Alpha controls the balance between:**
- **Recent data** (α)
- **Historical data** (1 - α)

| Alpha | Weight on Current | Weight on History | Behavior |
|-------|------------------|-------------------|----------|
| 0.1   | 10%              | 90%              | Very smooth, slow to adapt |
| **0.25**  | **25%**          | **75%**          | **Balanced (our choice)** |
| 0.5   | 50%              | 50%              | Responsive |
| 0.9   | 90%              | 10%              | Very reactive, noisy |

**We use α=0.25 because:**
- Smooth enough to ignore temporary spikes
- Responsive enough to detect real trends
- Proven optimal in C# reference implementation

## Real-World Example

Let's simulate 10 batches with varying performance:

```
Batch | Rows/s | EWMA Calculation                        | EWMA Result
------|--------|----------------------------------------|-------------
  1   | 10000  | Initialize: 10000                      | 10,000
  2   | 10500  | 0.25*10500 + 0.75*10000 = 2625 + 7500 | 10,125
  3   | 15000  | 0.25*15000 + 0.75*10125 = 3750 + 7594 | 11,344 (spike smoothed!)
  4   | 11000  | 0.25*11000 + 0.75*11344 = 2750 + 8508 | 11,258
  5   | 12000  | 0.25*12000 + 0.75*11258 = 3000 + 8444 | 11,444
  6   | 12500  | 0.25*12500 + 0.75*11444 = 3125 + 8583 | 11,708
  7   | 13000  | 0.25*13000 + 0.75*11708 = 3250 + 8781 | 12,031
  8   | 13200  | 0.25*13200 + 0.75*12031 = 3300 + 9023 | 12,323
  9   | 13500  | 0.25*13500 + 0.75*12323 = 3375 + 9242 | 12,617
 10   | 14000  | 0.25*14000 + 0.75*12617 = 3500 + 9463 | 12,963
```

**Notice:**
- Batch #3: Spike to 15,000 → EWMA only goes to 11,344 (smoothed)
- Batches #4-10: Gradual upward trend → EWMA follows steadily
- No wild fluctuations, stable metric for decision-making

## How It's Used in Adaptive Batching

The EWMA gives us a **stable throughput metric** for logging and monitoring:

```python
# After each successful batch:
rows_per_sec = 13500      # Current batch: 13,500 rows/s
ewma_rows_per_sec = 12617 # Smoothed average: 12,617 rows/s

log.info(f"Inserted 50000/1000000. Batch=10000, 2.15s "
         f"({rows_per_sec:.0f} rows/s, EWMA={ewma_rows_per_sec:.0f} rows/s)")
```

**Output:**
```
INFO: Inserted 50000/1000000. Batch=10000, 2.15s (13500 rows/s, EWMA=12617 rows/s)
```

**Interpretation:**
- **13,500 rows/s** = This specific batch (may be a spike)
- **12,617 rows/s** = Smoothed average (more reliable for overall progress)

## Key Benefits

1. **Reduces Noise** - Ignores temporary spikes/dips
2. **Detects Trends** - Still responds to sustained changes
3. **More Recent = More Weight** - Recent batches matter more than old ones
4. **Simple & Fast** - O(1) calculation, no need to store history
5. **Industry Standard** - Used in network monitoring, stock trading, DevOps

## Comparison: Simple Average vs EWMA

### Scenario: Performance degrades suddenly

**Simple Average (last 5 batches):**
```
[12000, 12000, 12000, 12000, 12000] → 12,000 rows/s
[12000, 12000, 12000, 12000, 6000]  → 10,800 rows/s  (sudden drop)
[12000, 12000, 12000, 6000, 6000]   → 9,600 rows/s
[12000, 12000, 6000, 6000, 6000]    → 8,400 rows/s
[12000, 6000, 6000, 6000, 6000]     → 7,200 rows/s   (still affected by old 12k)
```

**EWMA (α=0.25):**
```
EWMA = 12,000
New batch = 6,000 → EWMA = 0.25*6000 + 0.75*12000 = 10,500
New batch = 6,000 → EWMA = 0.25*6000 + 0.75*10500 = 9,375
New batch = 6,000 → EWMA = 0.25*6000 + 0.75*9375 = 8,531
New batch = 6,000 → EWMA = 0.25*6000 + 0.75*8531 = 7,898  (converges faster!)
```

EWMA adapts **faster** to the new normal while still being smooth.

## Mathematical Insight

The formula is **recursive** - each EWMA includes all previous history:

```
EWMA(t) = α * X(t) + (1-α) * EWMA(t-1)
        = α * X(t) + (1-α) * [α * X(t-1) + (1-α) * EWMA(t-2)]
        = α * X(t) + α(1-α) * X(t-1) + (1-α)² * EWMA(t-2)
        = α * [X(t) + (1-α)*X(t-1) + (1-α)²*X(t-2) + (1-α)³*X(t-3) + ...]
```

**Effective weights with α=0.25:**
- Current batch:   25.0%
- 1 batch ago:     18.8%  (0.75 * 0.25)
- 2 batches ago:   14.1%  (0.75² * 0.25)
- 3 batches ago:   10.5%  (0.75³ * 0.25)
- 4 batches ago:    7.9%  (0.75⁴ * 0.25)
- 5 batches ago:    5.9%  (0.75⁵ * 0.25)
- ...and so on (exponentially decaying)

Older batches still contribute, but with exponentially less weight!

## Summary

**EWMA provides:**
✅ Smooth, stable throughput metric
✅ Resilient to temporary spikes
✅ Responsive to sustained trends
✅ Simple, efficient calculation
✅ No need to store batch history

**Perfect for adaptive batching where we need:**
- Reliable performance metrics for logging
- Smooth decision-making (not reactive to noise)
- Real-time calculation with minimal overhead
