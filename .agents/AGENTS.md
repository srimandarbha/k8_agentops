# Operator Development Guidelines

These rules dictate the best practices and strict design patterns to follow when developing the SRE Operator with Kubebuilder. They are designed to prevent common scale, performance, and stability issues in production.

## 1. Reconciliation Loop & Idempotency
- **Level-Based Thinking**: Reconcilers must operate on the *current* state of the cluster versus the desired state, regardless of what triggered the reconcile. Do not assume edge-triggered events.
- **Strict Idempotency**: Every reconcile loop must be safe to execute multiple times. When updating or creating child resources, use `controllerutil.SetControllerReference` and prefer patch (e.g., `client.MergeFrom()`) over `Update()` to minimize `ResourceVersion` conflicts.

## 2. Error Handling & Requeue Strategies
- **Classify errors strictly**:
  - *Transient* (network, rate limits): Do not return the error. Manage backoff explicitly: `return ctrl.Result{RequeueAfter: 5 * time.Second}, nil`.
  - *Conflicts* (`apierrors.IsConflict`): Retry immediately: `return ctrl.Result{Requeue: true}, nil`.
  - *NotFound* (missing dependencies): Wait and retry: `return ctrl.Result{RequeueAfter: 30 * time.Second}, nil`.
  - *Terminal Errors* (bad spec): Update the status to `Degraded`, and *never* retry: `return ctrl.Result{}, nil`.
- **Prevent Requeue Storms**: Never return `ctrl.Result{Requeue: true}, nil` unconditionally. Always use `RequeueAfter` or let the controller-runtime exponential backoff handle unknown errors.

## 3. Finalizers, Deletion & Garbage Collection
- **Deletion Check**: Always check `!obj.DeletionTimestamp.IsZero()` first before doing any reconciliation logic.
- **Immediate Return**: If adding a finalizer, do it early, update the object, and **immediately return** (do not continue reconciliation in the same tick).
- **Deadlock Prevention**: Ensure your cleanup logic handles external dependencies being down. Always implement a force-delete escape hatch (e.g., via a specific annotation).
- **Owner References**: Always use `controllerutil.SetControllerReference` for resources the operator manages to ensure proper garbage collection. Remember that cross-namespace owner references are NOT supported by Kubernetes GC.

## 4. Status Management & CRD Design
- **Conditions Pattern**: Use the standard `metav1.Condition` pattern for status fields (`Ready`, `Degraded`, `Progressing`).
- **Observed Generation**: Always set `ObservedGeneration: obj.Generation` so clients know if the status reflects the latest spec.
- **Status Subresource**: Update status independently of spec using `r.Status().Patch(...)`. Only patch if the status has actually changed to prevent infinite watch loops.
- **Size Limits**: Never write large amounts of data (like log arrays or full metric payloads) into the CRD status (etcd limit is 1.5MB). Use `ConfigMaps` and reference them by name in the status instead.
- **CRD Versioning**: Plan for conversion webhooks if migrating from `v1alpha1` to `v1beta1`. Never rename or remove required fields in stable API versions.

## 5. Cache Optimization, Informers & Performance
- **Read from Cache**: **Never bypass the cache** for reads unless absolutely necessary. `r.Get()` and `r.List()` read from the local informer cache.
- **Field Indexers**: Do not perform `O(N)` linear scans on lists to filter by properties (e.g., matching an owner). Register Field Indexers via `mgr.GetFieldIndexer().IndexField` during manager setup.
- **Watch Predicates**: Use predicates (`GenerationChangedPredicate`, `LabelChangedPredicate`) to aggressively filter watch events and prevent infinite loop reconciles (e.g., ignoring pure status updates).
- **DeepCopy Discipline**: Cache objects are read-only. Only use `.DeepCopy()` when you are about to mutate the object for an update/patch, avoiding unnecessary memory allocations when just reading.
- **List Pagination**: Always use `client.Limit(100)` when listing large collections to avoid overwhelming the API server.

## 6. Webhooks & Validation
- **No External Calls**: Validation webhooks must be `O(1)`. **Never** make external API calls (e.g., to Vault or Prometheus) within a webhook handler.
- **Idempotent Mutation**: Mutation webhooks must be strictly idempotent (e.g., do not blindly append to arrays every time they are called).
- **Failsafe Configuration**: Use a 3-second timeout (`timeoutSeconds: 3`) instead of the 10s default. Prefer `failurePolicy: Ignore` over `Fail` for non-critical tasks so operator crashes do not lock the cluster. Exclude the operator's namespace.
- **CEL Validation**: Prefer CEL rules (`+kubebuilder:validation:XValidation`) over webhooks for simple cross-field validations.

## 7. Concurrency, Memory & Goroutine Leaks
- **Context Cancellation**: Every background goroutine must listen for `<-ctx.Done()` and gracefully exit when the manager shuts down.
- **Bounded Caches**: Unbounded maps or caches are forbidden; use fixed-size LRU caches if necessary (e.g., `hashicorp/golang-lru`).
- **Rate Limiting**: Configure `MaxConcurrentReconciles` and a custom `RateLimiter` (e.g., Token Bucket) on the controller options to prevent thundering herds on startup or during mass-requeues.

## 8. Logging & RBAC
- **Structured Logging**: Never use `fmt.Println` or the standard `log` package. Use `logr` (e.g., `log := log.FromContext(ctx)`). Include key-value pairs for structured Splunk/ELK parsing. Keep high-frequency operational logs at `V(1)` or `V(2)`.
- **Least Privilege**: Adhere to the principle of least privilege in RBAC markers (`+kubebuilder:rbac:groups=...`). Never use wildcards (`*`). Do not grant `delete` permissions on PVCs or `patch` permissions on external Secrets.

## 9. Testing & Deployment
- **Integration Testing**: Use `envtest` for integration tests with a real API server binary. Do not mock the API server.
- **Unit Testing**: Use the controller-runtime `fake` client for testing reconciliation logic rapidly.
- **Production Deployment Readiness**:
  - Enable Leader Election (`replicas >= 2`).
  - Configure `PodDisruptionBudget` (`minAvailable: 1`).
  - Set `terminationGracePeriodSeconds: 60` to allow in-flight reconciles to finish.
  - Expose `/healthz` and `/readyz` probes.
