# Appendix F: Process portability and trust boundaries

## 1. Processes are an explicit semantic choice

Thread and process execution can return equal values while still having different observable effects. Threads share application memory; a process operates in a different address space. A function that mutates a global cache, relies on a thread-local object, or uses an inherited open connection is not interchangeable merely because its return value is picklable.

`process_safe=True` is the caller's declaration that process execution is acceptable. It is not a dynamic proof, importability test, security label, or speed prediction. Leave the declaration false until the application has reviewed both data transfer and effects.

The [official multiprocessing guidance](https://docs.python.org/3.13/library/multiprocessing.html) explains process start methods and import requirements. This project deliberately uses `spawn` when `mp_context` is omitted so that its default does not depend on the operating system's changing start-method choice.

## 2. A portability checklist

Before enabling a process route, verify all of the following in a normal importable module:

- The submitted callable is accessible to the child interpreter.
- Its arguments and return value are serializable by the selected backend.
- The module can be imported without starting new pools or performing unintended work.
- Startup is guarded by `if __name__ == "__main__":` in executable scripts.
- Dependencies and input files are available to the child with the intended paths.
- Process-local state is initialized deliberately rather than assumed to match a live thread.
- Errors and shutdown behavior are tested on the deployment operating systems.

Top-level module functions with simple immutable inputs are a good first fixture. Lambdas, nested closures, interactive definitions, and objects holding live locks or sockets deserve separate scrutiny. A successful `pickle.dumps` of an argument alone would not prove that a worker can import its callable, safely execute it, or return its result.

## 3. Why notebooks need special care

A notebook cell is not automatically an importable worker module. A function that runs in a thread from an interactive environment may fail when a spawned worker tries to resolve it. Put process worker functions in an importable `.py` module and run a guarded entry point for the first smoke test.

Do not hide such failures by silently resubmitting the task to a thread. A fallback can change shared-state effects, duplicate external work after ambiguous failure, and conceal the importability defect. The executor surfaces the backend failure and leaves the next action to the caller.

## 4. Choosing a different context

The constructor accepts `mp_context` for applications that deliberately choose another available start method. That choice is an application-level responsibility; this library does not globally call `set_start_method` or alter other libraries' process configuration.

Compare contexts in separate fresh processes. Import and startup costs are part of the cold-start result, and worker-state inheritance can differ. A configuration that happens to work with inherited state should not be called portable without a spawn test. Forking a multithreaded application has additional risks documented by Python, so “fork was faster in one trial” is not sufficient justification for a service-wide change.

Do not change the process start method to evade serialization requirements. Pool tasks still cross a task transport boundary, and a child must execute a valid callable under the chosen backend contract.

## 5. Data movement can erase parallelism gains

A useful reasoning model is:

```text
process path ≈ startup share + queue wait + argument transfer
             + useful work + result transfer + completion handling
thread path  ≈ queue wait + useful work under thread contention
             + completion handling
```

These terms are a planning model, not a measured decomposition of the executor profile. If one operation touches a very large buffer for only a small amount of work, transfer can dominate. If it reuses the same large immutable dataset repeatedly, explicit worker initialization, affinity, or shared-memory descriptors may eventually help. Those are separate planned features, not inferred from `process_safe`.

Do not send a huge mutable object merely because the backend accepts it. Consider moving an immutable small descriptor and opening data inside a controlled worker function, provided that the resulting filesystem access and lifetime are appropriate. Benchmark the complete path, including file reads or cleanup, rather than subtracting inconvenient costs.

## 6. Native libraries and runtime variants

CPU-heavy does not always mean a process is best. Some numerical, compression, image, or cryptographic operations spend most of their time in native code. Their thread behavior and internal parallelism depend on the library and build. If each process starts several native threads, total runnable concurrency can greatly exceed the executor worker count.

Record native-library thread settings alongside pool sizes. Where a library requires settings before import, configure them in the application's startup rather than changing global state opportunistically during scheduling. A resource-budget extension would need an explicit contract about what it can actually control.

[PEP 703](https://peps.python.org/pep-0703/) motivates testing free-threaded builds separately. This repository does not assume that all supported Python builds have the same GIL behavior, nor does it guarantee arbitrary task code is thread-safe on a free-threaded build. Interpreter-pool and automatic free-threaded-aware policies remain planned; the current runtime backends are threads and processes.

## 7. Failure scope

An application exception is normally delivered as the task's failed future. Abrupt worker termination can break the process pool and affect other pending tasks. A successful task may already have produced external effects before a worker or transport failure becomes visible.

There is no automatic retry or worker-recovery service in the base executor. Repeating a task is an application decision requiring knowledge of idempotency. If a future reports failure after an external write, do not infer the write did not happen. Durable acknowledgements and exactly-once effects require a different protocol with the destination.

## 8. Security boundary

The process pool is for trusted application code. It is not a sandbox for code from an unknown user, generated plugins, or data requiring mutually untrusted tenant isolation. Workers normally run with the application's user permissions and can access resources available to that user. Serialization is not a safe exchange format for untrusted executable objects.

The policy registry likewise creates trusted Python objects in the application process. The concept registry is different: it loads and queries descriptive JSON and does not execute a listed concept. Keep those two registries conceptually separate.

Activepieces' [isolation documentation](https://www.activepieces.com/docs/install/guarantees/execution-isolation) is useful inspiration for defining boundaries, not evidence that these local pools provide them. Any future hostile-code feature needs a separate threat model, isolation layer, resource limits, and independent review.

## 9. Release matrix

At minimum, a release targeting multiple operating systems should exercise:

| Dimension | Suggested cases |
| --- | --- |
| Runtime | Minimum supported Python and current supported versions |
| OS | Linux, macOS, Windows where support is claimed |
| Context | Project default spawn; other contexts only if explicitly supported |
| Payload | Small primitives, nontrivial serializable object, deliberate invalid object |
| Results | Normal value, task exception, deliberate invalid result |
| Lifecycle | Startup, repeated batches, queued cancellation, shutdown, abrupt worker failure |
| Packaging | Installed wheel used by an importable worker module |

This is a release checklist. It does not assert that the current repository has already passed every cell. Record actual test evidence and explicit skips in CI or release notes.
