"""A transparent adaptive layer on concurrent.futures. Prototype, not magic AI."""
from .catalog import ConceptRegistry
from .executor import BackpressureError, IntelligentExecutor
from .policies import AdaptivePolicy, HintPolicy, Policy, PolicyRegistry, Profile, TaskHints, ThreadPolicy

__version__ = "0.1.0"
__all__ = ["ConceptRegistry", "IntelligentExecutor", "BackpressureError", "TaskHints", "Profile", "Policy", "PolicyRegistry", "AdaptivePolicy", "HintPolicy", "ThreadPolicy"]
