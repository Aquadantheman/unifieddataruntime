# Speculative Safety: A Formal Proof of Serializability Preservation

---

## Abstract

Speculative execution in distributed transactions offers dramatic latency improvements by committing locally before obtaining global consensus. However, this optimism introduces risk: if a conflict is later discovered, the speculative commit must be rolled back. We present a formal proof that speculative execution preserves serializability under precise conditions. We define a formal model of speculative transaction states, prove that Bloom filter conflict detection guarantees detection completeness (zero false negatives), establish rollback atomicity, and show that the resulting transaction history is serializable. Our key result is the **Speculative Safety Theorem**: if (1) conflict detection has zero false negatives, (2) rollback completes before speculative effects become visible, and (3) confirmed commits are totally ordered, then speculative execution produces serializable histories.

**Keywords**: speculative execution, serializability, distributed transactions, formal verification, conflict detection

---

## 1. Introduction

### 1.1 The Speculation Opportunity

Distributed consensus protocols like two-phase commit (2PC), Paxos, and Raft guarantee agreement but impose latency costs. For a transaction requiring $R$ round-trips at latency $L$:

$$T_{consensus} = R \cdot L$$

At geo-distributed latencies (50-150ms RTT), this dominates transaction time. Yet empirically, most transactions do not conflict—they could commit immediately without waiting for consensus.

**Speculative execution** exploits this observation: commit locally first, confirm asynchronously. If a conflict is discovered, roll back. The expected latency becomes:

$$E[T_{speculative}] = T_{local} + p \cdot T_{recovery}$$

Where $p$ is the conflict probability. When $p \ll 1$, speculation dramatically outperforms consensus.

### 1.2 The Safety Question

The critical question is: **does speculative execution preserve serializability?**

A naive implementation could violate isolation:
1. Transaction $T_1$ speculatively commits, writing $x = 1$
2. Transaction $T_2$ reads $x = 1$ (seeing speculative state)
3. Conflict detected: $T_1$ must roll back
4. But $T_2$ has already committed based on $T_1$'s rolled-back value

This "dirty read" violates serializability. Our proof establishes conditions that prevent such anomalies.

### 1.3 Contributions

1. **Formal Model**: Precise definitions of transaction states, visibility, and speculative commits
2. **Detection Completeness Theorem**: Proof that Bloom filter conflict detection has zero false negatives
3. **Rollback Atomicity Theorem**: Proof that rollback removes all speculative effects atomically
4. **Speculative Safety Theorem**: Proof that under our conditions, speculative execution preserves serializability
5. **Visibility Invariant**: The key property that prevents dirty reads

---

## 2. System Model

### 2.1 Transactions

**Definition 1 (Transaction)**: A transaction $T$ is a sequence of operations:

$$T = \langle op_1, op_2, \ldots, op_n, \text{commit} \rangle$$

where each $op_i$ is either $\text{read}(x)$ or $\text{write}(x, v)$ for some key $x$ and value $v$.

**Definition 2 (Write Set)**: The write set of transaction $T$, denoted $W(T)$, is the set of keys written by $T$:

$$W(T) = \{x : \text{write}(x, v) \in T \text{ for some } v\}$$

**Definition 3 (Read Set)**: The read set of transaction $T$, denoted $R(T)$, is the set of keys read by $T$:

$$R(T) = \{x : \text{read}(x) \in T\}$$

### 2.2 Conflict

**Definition 4 (Conflict)**: Two transactions $T_1$ and $T_2$ conflict if their write sets intersect:

$$\text{conflict}(T_1, T_2) \iff W(T_1) \cap W(T_2) \neq \emptyset$$

For read-write conflicts (snapshot isolation), we extend:

$$\text{conflict}_{SI}(T_1, T_2) \iff W(T_1) \cap W(T_2) \neq \emptyset \lor W(T_1) \cap R(T_2) \neq \emptyset$$

Our proof applies to write-write conflict detection; extending to read-write is straightforward.

### 2.3 Transaction States

**Definition 5 (Transaction State)**: A transaction exists in one of four states:

$$\text{State} \in \{\text{Active}, \text{Speculative}, \text{Confirmed}, \text{Aborted}\}$$

State transitions:

```
Active → Speculative    (speculative commit)
Active → Confirmed      (eager commit, bypassing speculation)
Active → Aborted        (explicit abort or conflict during active)
Speculative → Confirmed (no conflict detected, confirmation received)
Speculative → Aborted   (conflict detected, rollback triggered)
```

**Definition 6 (Confirmed Set)**: At time $t$, the confirmed set $C_t$ is the set of all transactions in state Confirmed.

**Definition 7 (Speculative Set)**: At time $t$, the speculative set $S_t$ is the set of all transactions in state Speculative.

### 2.4 Visibility

**Definition 8 (Visibility)**: A write $\text{write}(x, v)$ by transaction $T$ is visible to transaction $T'$ at time $t$ if and only if:

$$\text{visible}(T, T', t) \iff T \in C_t$$

**Key Property**: Speculative writes are NOT visible. Only confirmed writes are visible.

This is the critical invariant that prevents dirty reads.

### 2.5 History and Serializability

**Definition 9 (History)**: A history $H$ is a partial order of operations from a set of transactions, respecting each transaction's internal order.

**Definition 10 (Serial History)**: A history is serial if all operations of each transaction are contiguous (no interleaving).

**Definition 11 (Serializable)**: A history $H$ is serializable if it is equivalent to some serial history $H_s$. Equivalence means the same operations produce the same final state.

**Definition 12 (Commit Order)**: The commit order $\prec_c$ is a total order on confirmed transactions based on their confirmation timestamp.

---

## 3. Speculative Execution Protocol

### 3.1 Protocol Description

The speculative execution protocol proceeds as follows:

**Phase 1: Speculative Commit**
```
SPECULATIVE_COMMIT(T):
    1. Validate T locally (constraints, types)
    2. Compute write set W(T)
    3. Estimate conflict probability p
    4. If p < threshold:
        a. Record T's writes in speculative buffer
        b. Set state(T) = Speculative
        c. Assign tentative timestamp ts(T)
        d. Return "committed (speculative)" to client
    5. Else:
        a. Proceed with eager consensus
```

**Phase 2: Conflict Detection (Background)**
```
DETECT_CONFLICTS(T):
    1. For each transaction T' where state(T') = Speculative ∧ T' ≠ T:
        a. If detect(W(T), W(T')) = true:
            Return CONFLICT(T, T')
    2. For each transaction T' where state(T') = Confirmed ∧ ts(T') > ts_start(T):
        a. If detect(W(T), W(T')) = true:
            Return CONFLICT(T, T')
    3. Return NO_CONFLICT
```

**Phase 3: Confirmation or Rollback**
```
RESOLVE(T, detection_result):
    1. If detection_result = NO_CONFLICT:
        a. Move T's writes from speculative buffer to confirmed store
        b. Set state(T) = Confirmed
        c. Assign final timestamp ts_final(T)
        d. Notify client "confirmed"
    2. If detection_result = CONFLICT(T, T'):
        a. Remove T's writes from speculative buffer
        b. Set state(T) = Aborted
        c. Notify client "aborted, please retry"
```

### 3.2 Speculative Buffer Isolation

**Definition 13 (Speculative Buffer)**: The speculative buffer $B_s$ is a data structure holding writes from speculative transactions, isolated from the confirmed store.

**Property 1 (Buffer Isolation)**: Reads by any transaction $T'$ access only the confirmed store, never the speculative buffer:

$$\text{read}(x) \text{ by } T' \Rightarrow \text{value from confirmed store only}$$

This property is enforced by the system implementation and is the mechanism by which the visibility invariant (Definition 8) is maintained.

---

## 4. Detection Completeness

### 4.1 Bloom Filter Conflict Detection

We use Bloom filters for efficient conflict detection. A Bloom filter $BF$ is a probabilistic data structure supporting:
- $\text{insert}(BF, x)$: Add element $x$
- $\text{query}(BF, x)$: Returns "possibly present" or "definitely not present"

**Lemma 1 (Bloom Filter No False Negatives)**: For any Bloom filter $BF$ and element $x$:

$$\text{insert}(BF, x) \Rightarrow \text{query}(BF, x) = \text{possibly present}$$

*Proof*: The insert operation sets $k$ bits at positions $h_1(x), h_2(x), \ldots, h_k(x)$. The query operation checks if all $k$ bits are set. Since bits are never cleared (Bloom filters are insert-only), any inserted element will have all its bits set, and the query will return "possibly present". □

### 4.2 Write Set Encoding

For conflict detection, we encode each transaction's write set as a Bloom filter:

$$BF(T) = \text{Bloom filter containing all } x \in W(T)$$

**Definition 14 (Bloom Conflict Detection)**:

$$\text{detect}_{BF}(T_1, T_2) = \exists x \in W(T_1) : \text{query}(BF(T_2), x) = \text{possibly present}$$

### 4.3 Detection Completeness Theorem

**Theorem 1 (Detection Completeness)**: If two transactions $T_1$ and $T_2$ conflict, Bloom filter detection will detect the conflict:

$$\text{conflict}(T_1, T_2) \Rightarrow \text{detect}_{BF}(T_1, T_2) = \text{true}$$

*Proof*:

Assume $\text{conflict}(T_1, T_2)$. By Definition 4:
$$W(T_1) \cap W(T_2) \neq \emptyset$$

Let $x \in W(T_1) \cap W(T_2)$.

Since $x \in W(T_2)$, we have $\text{insert}(BF(T_2), x)$ was executed.

By Lemma 1:
$$\text{query}(BF(T_2), x) = \text{possibly present}$$

Since $x \in W(T_1)$, when checking $T_1$ against $BF(T_2)$:
$$\exists x \in W(T_1) : \text{query}(BF(T_2), x) = \text{possibly present}$$

By Definition 14:
$$\text{detect}_{BF}(T_1, T_2) = \text{true}$$

□

**Corollary 1 (No Missed Conflicts)**: Bloom filter conflict detection never misses a real conflict. It may produce false positives (detecting conflict where none exists) but never false negatives.

---

## 5. Rollback Atomicity

### 5.1 Rollback Operation

**Definition 15 (Rollback)**: The rollback of speculative transaction $T$ is the operation:

$$\text{rollback}(T) = \text{remove all of } T\text{'s writes from speculative buffer } B_s$$

### 5.2 Atomicity Requirement

For safety, rollback must be atomic with respect to visibility:

**Definition 16 (Atomic Rollback)**: Rollback is atomic if there exists no time $t$ during the rollback operation where a partial set of $T$'s writes is in $B_s$.

Formally, if rollback begins at time $t_1$ and completes at time $t_2$:
- At all $t < t_1$: All of $T$'s writes are in $B_s$
- At all $t \geq t_2$: None of $T$'s writes are in $B_s$
- At all $t_1 \leq t < t_2$: The system does not process any reads (rollback is blocking)

### 5.3 Rollback Atomicity Theorem

**Theorem 2 (Rollback Atomicity)**: Under the speculative buffer isolation property (Property 1), rollback atomicity is not required for safety.

*Proof*:

The visibility invariant (Definition 8) states that speculative writes are never visible to other transactions. By Property 1, reads access only the confirmed store.

Therefore, regardless of the state of the speculative buffer during rollback:
- No transaction $T'$ can read from $B_s$
- No transaction $T'$ can observe partial rollback state
- The rollback operation's atomicity is irrelevant to external observers

Rollback need only be atomic with respect to the confirmation decision: if a conflict is detected, the transaction must not be confirmed.

□

**Corollary 2 (Simplified Rollback)**: The speculative buffer can be rolled back non-atomically (e.g., deleting writes one by one) without violating safety, because speculative writes are never visible.

---

## 6. Serializability Preservation

### 6.1 The Speculative Safety Theorem

**Theorem 3 (Speculative Safety)**: If the following conditions hold:

1. **Detection Completeness**: Conflict detection has zero false negatives
2. **Visibility Invariant**: Only confirmed writes are visible
3. **Commit Order**: Confirmed transactions have a total order $\prec_c$

Then speculative execution produces serializable histories.

*Proof*:

We prove by construction that any history $H$ produced by the speculative protocol is equivalent to a serial history $H_s$.

**Step 1: Partition transactions**

Let $H$ be a history over transactions $\{T_1, T_2, \ldots, T_n\}$.

Partition into:
- $C = \{T : \text{state}(T) = \text{Confirmed}\}$ (confirmed transactions)
- $A = \{T : \text{state}(T) = \text{Aborted}\}$ (aborted transactions)
- $S = \{T : \text{state}(T) = \text{Speculative}\}$ (still speculative at end of history)

**Step 2: Aborted transactions have no effect**

For any $T \in A$:
- By the visibility invariant, $T$'s writes were never visible
- By rollback, $T$'s writes are removed from $B_s$
- Therefore, $T$ has no effect on the final state
- We can remove $T$ from $H$ without changing equivalence

**Step 3: Speculative transactions have no effect (on visible state)**

For any $T \in S$:
- By the visibility invariant, $T$'s writes are not visible
- Other transactions cannot have read from $T$
- Therefore, $T$ has no effect on the observable history
- We can remove $T$ from $H$ without changing equivalence

**Step 4: Confirmed transactions are conflict-free**

For any $T_i, T_j \in C$ where $i \neq j$:
- Both were confirmed, meaning no conflict was detected
- By Theorem 1 (Detection Completeness), if they conflicted, detection would have found it
- By contrapositive: $\neg \text{detect}(T_i, T_j) \Rightarrow \neg \text{conflict}(T_i, T_j)$

Wait—this is not quite right. Detection completeness says:
$$\text{conflict}(T_i, T_j) \Rightarrow \text{detect}(T_i, T_j)$$

The contrapositive is:
$$\neg \text{detect}(T_i, T_j) \Rightarrow \neg \text{conflict}(T_i, T_j)$$

So if both $T_i$ and $T_j$ were confirmed, neither detected a conflict with the other, meaning they don't conflict.

**Step 5: Construct serial order**

Since confirmed transactions don't conflict:
$$\forall T_i, T_j \in C : W(T_i) \cap W(T_j) = \emptyset$$

Their writes are to disjoint sets of keys. We can order them by commit order $\prec_c$:

$$H_s = T_{\pi(1)}, T_{\pi(2)}, \ldots, T_{\pi(|C|)}$$

where $\pi$ is the permutation induced by $\prec_c$.

**Step 6: Equivalence**

$H_s$ is serial by construction. We show $H \equiv H_s$:

- Same transactions execute (we removed only aborted and speculative)
- Same final state: Since writes are to disjoint keys, order doesn't matter
- Same read values: Each read sees the latest confirmed write to that key, which is the same in any ordering of non-conflicting transactions

Therefore $H$ is serializable.

□

### 6.2 Key Insight

The proof reveals the key insight: **speculative execution reduces to non-conflicting concurrent execution** for confirmed transactions. The speculation mechanism is a performance optimization—it doesn't change the semantics of confirmed transactions.

The visibility invariant is the critical safety mechanism: by ensuring speculative writes are never visible, we prevent cascading aborts and dirty reads.

---

## 7. Liveness Analysis

Safety alone is insufficient; we must also consider liveness: will transactions eventually confirm?

### 7.1 Progress Guarantee

**Theorem 4 (Progress)**: If conflict probability $p < 1$, speculative transactions eventually confirm with probability 1.

*Proof*:

For a transaction $T$:
- If $T$ conflicts with existing transactions, it is aborted
- $T$ can retry
- Each retry has independent conflict probability $p$
- Probability of confirming after $k$ retries: $1 - p^k$
- As $k \to \infty$: $\lim_{k \to \infty} (1 - p^k) = 1$ for $p < 1$

Therefore, with probability 1, $T$ eventually confirms.

□

### 7.2 Expected Retries

**Corollary 3 (Expected Retries)**: The expected number of attempts before confirmation is:

$$E[\text{attempts}] = \frac{1}{1-p}$$

For $p = 0.01$ (1% conflict rate): $E[\text{attempts}] = 1.01$
For $p = 0.10$ (10% conflict rate): $E[\text{attempts}] = 1.11$
For $p = 0.50$ (50% conflict rate): $E[\text{attempts}] = 2.00$

### 7.3 When to Speculate

Combining with the latency analysis from POAC:

$$E[T_{speculative}] = T_{local} + p \cdot (T_{rollback} + T_{retry})$$

Speculation is beneficial when:
$$E[T_{speculative}] < T_{eager}$$
$$T_{local} + p \cdot (T_{rollback} + T_{retry}) < T_{local} + T_{consensus}$$
$$p < \frac{T_{consensus}}{T_{rollback} + T_{retry}}$$

For typical values ($T_{consensus} = 100ms$, $T_{rollback} + T_{retry} = 10ms$):
$$p < 10 = 1000\%$$

Speculation is beneficial for any realistic conflict rate when consensus latency is high.

---

## 8. Implementation Considerations

### 8.1 Visibility Invariant Enforcement

The visibility invariant must be enforced at the system level:

1. **Separate Storage**: Speculative buffer physically separate from confirmed store
2. **Read Path**: Reads always go to confirmed store, never speculative buffer
3. **Confirmation**: Atomic move from speculative buffer to confirmed store
4. **No Peeking**: No API to read speculative state externally

### 8.2 Conflict Detection Timing

Conflicts can be detected at two times:

1. **At speculative commit**: Check against currently speculative and recently confirmed transactions
2. **At confirmation time**: Re-check against transactions confirmed since speculative commit

Both are necessary for complete coverage:
- At-commit detection catches conflicts with concurrent transactions
- At-confirmation detection catches conflicts with transactions that confirmed during the speculation window

### 8.3 Bloom Filter Sizing

For a target false positive rate $p_{FP}$ with $n$ elements:

$$m = \frac{-n \ln p_{FP}}{(\ln 2)^2} \text{ bits}$$
$$k = \frac{m}{n} \ln 2 \text{ hash functions}$$

False positives cause unnecessary aborts (hurting performance) but don't affect safety.

---

## 9. Conclusion

We have formally proven that speculative execution preserves serializability under three conditions:

1. **Detection Completeness**: Conflict detection must have zero false negatives
2. **Visibility Invariant**: Speculative writes must not be visible to other transactions
3. **Commit Order**: Confirmed transactions must have a total order

The proof reveals that speculation is fundamentally a performance optimization: confirmed transactions are conflict-free by construction, and their order is determined by the commit order. The visibility invariant prevents dirty reads and cascading aborts.

Bloom filters provide an efficient implementation of conflict detection with guaranteed completeness (Theorem 1). The speculative buffer isolation enforces the visibility invariant. Standard timestamp assignment provides commit ordering.

Together, these results establish that speculative execution is not merely a heuristic but a provably correct optimization for distributed transactions.

---

## References

[1] Kung, H. T., & Robinson, J. T. (1981). On Optimistic Methods for Concurrency Control. ACM TODS. doi:10.1145/319566.319567

[2] Bernstein, P. A., Hadzilacos, V., & Goodman, N. (1987). Concurrency Control and Recovery in Database Systems. Addison-Wesley.

[3] Bloom, B. H. (1970). Space/Time Trade-offs in Hash Coding with Allowable Errors. CACM. doi:10.1145/362686.362692

[4] Herlihy, M., & Wing, J. M. (1990). Linearizability: A Correctness Condition for Concurrent Objects. ACM TOPLAS. doi:10.1145/78969.78972

[5] Papadimitriou, C. H. (1979). The Serializability of Concurrent Database Updates. Journal of the ACM. doi:10.1145/322154.322158

[6] Lamport, L. (1978). Time, Clocks, and the Ordering of Events in a Distributed System. CACM. doi:10.1145/359545.359563

---

## Appendix A: Formal State Machine

### A.1 Transaction State Machine

```
States: {Active, Speculative, Confirmed, Aborted}

Initial State: Active

Transitions:
  Active --[speculative_commit]--> Speculative
    Precondition: p_estimated < threshold
    Action: Add writes to speculative buffer

  Active --[eager_commit]--> Confirmed
    Precondition: p_estimated >= threshold OR bypass_speculation
    Action: Add writes to confirmed store

  Active --[abort]--> Aborted
    Precondition: explicit_abort OR validation_failure
    Action: Discard transaction

  Speculative --[confirm]--> Confirmed
    Precondition: no_conflict_detected
    Action: Move writes from speculative buffer to confirmed store

  Speculative --[rollback]--> Aborted
    Precondition: conflict_detected
    Action: Remove writes from speculative buffer

Terminal States: {Confirmed, Aborted}
```

### A.2 Invariants

**I1 (Visibility)**: $\forall T, T' : \text{visible}(T, T') \Rightarrow T \in C$

**I2 (Conflict-Free Confirmed)**: $\forall T_1, T_2 \in C : W(T_1) \cap W(T_2) = \emptyset$

**I3 (Total Order)**: $\forall T_1, T_2 \in C : T_1 \prec_c T_2 \lor T_2 \prec_c T_1$

---

## Appendix B: Comparison with Traditional OCC

### B.1 Standard Optimistic Concurrency Control

Traditional OCC (Kung-Robinson):
1. Read phase: Execute locally, track read/write sets
2. Validation phase: Check for conflicts with committed transactions
3. Write phase: If valid, commit; else abort and retry

### B.2 Speculative Execution Differences

| Aspect | Traditional OCC | Speculative Execution |
|--------|-----------------|----------------------|
| Commit timing | After validation | Before confirmation |
| Client return | After write phase | After speculative commit |
| Conflict window | Read phase to validation | Speculative to confirmation |
| Rollback complexity | Discard local writes | Remove from speculative buffer |
| Cascading aborts | Not possible (no visibility) | Not possible (visibility invariant) |

### B.3 Key Advantage

Speculative execution returns to the client earlier than OCC:
- OCC: Return after validation + write phase
- Speculation: Return immediately after local validation

The confirmation happens asynchronously, reducing perceived latency.
