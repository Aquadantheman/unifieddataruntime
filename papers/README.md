# Research Papers

Technical papers documenting Rhizo's theoretical foundations and implementation.

## Papers

| Paper | Description | Status |
|-------|-------------|--------|
| [cross_table_acid_paper.md](cross_table_acid_paper.md) | Cross-table ACID transactions via content-addressable storage | Core implementation |
| [acid_without_consensus_paper.md](acid_without_consensus_paper.md) | Algebraic transactions for coordination-free distributed commits | Implemented |
| [waiting_waste_theorem_paper.md](waiting_waste_theorem_paper.md) | Why consensus energy diverges with network latency | Academic submission |
| [speculative_safety_proof.md](speculative_safety_proof.md) | Formal proof of serializability preservation under speculation | Implemented |
| [time_energy_theory_paper.md](time_energy_theory_paper.md) | Time and energy costs of coordination in distributed systems | Research |
| [poac_paper.md](poac_paper.md) | Probabilistic Optimistic Algebraic Consistency | Partially implemented |

## Summary

### Cross-Table ACID (Implemented)

The foundational paper describing how Rhizo achieves multi-table ACID transactions without a coordination service. Key contributions:
- Content-addressable storage with BLAKE3 hashing
- O(t) complexity for t-table transactions
- Zero-copy branching via pointer manipulation
- 1,500+ MB/s write throughput on commodity hardware

### ACID Without Consensus (Implemented)

Describes how algebraic operation classification enables coordination-free distributed transactions. Key contributions:
- Operations classified by algebraic structure (semilattice, Abelian group)
- Local commits for algebraically conflict-free operations
- **160,000x** measured vs cross-continent 2PC (NYC → Oregon + Ireland, real network)
- **30,000x** measured vs same-region 2PC (NYC → Virginia, real network)
- **59x** measured vs localhost 2PC; **355x** vs SQLite FULL sync
- 97,943x energy reduction vs consensus energy model (estimated)

### Waiting Waste Theorem (Academic Submission)

Focused paper proving why consensus energy diverges with network latency. Key contributions:
- **Waiting Waste Theorem**: $\lim_{L \to \infty} E_{wait} / E_{total} = 1$
- **Coordination-Free Corollary**: $\lim_{L \to \infty} E_{cf} / E_{consensus} = 0$
- Quantitative analysis showing 98%+ energy wasted at WAN latencies
- Empirical validation with 97,943x energy improvement measured

### Speculative Safety Proof (Implemented)

Formal proof that speculative execution preserves serializability. Key contributions:
- **Detection Completeness Theorem**: Bloom filters guarantee zero false negatives
- **Rollback Atomicity Theorem**: Visibility invariant makes atomicity unnecessary
- **Speculative Safety Theorem**: Three conditions for provably correct speculation
- Formal state machine and invariants for implementation

**Implementation:** `SpeculativeBuffer` with visibility invariant, `ConflictProbabilityTracker` with EMA learning, and `commit_with_speculation()` in `CoordinationFreeManager`. 11 tests validate the formal guarantees.

### POAC (Research/Partially Implemented)

Explores probabilistic techniques for further performance optimization:
- **Bloom filter write-sets** [Implemented] — O(1) conflict detection with POAC Table 1 validation
- **Speculative execution** [Implemented] — Adaptive threshold with EMA probability learning
- **Escrow transactions** [Future] — Hot spot scalability via pre-allocated quotas

## Citation

These papers are working drafts. If referencing this work, please cite the GitHub repository:

```
Rhizo: Data, connected.
https://github.com/rhizodata/rhizo
```

## Related Documentation

- [TECHNICAL_FOUNDATIONS.md](../docs/TECHNICAL_FOUNDATIONS.md) - Verified mathematical proofs
- [PERFORMANCE.md](../docs/PERFORMANCE.md) - Benchmark methodology and results
