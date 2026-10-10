"""A transparent adaptive layer on concurrent.futures. Prototype, not magic AI."""
from .catalog import ConceptRegistry
from .executor import BackpressureError, IntelligentExecutor
from .policies import AdaptivePolicy, HintPolicy, Policy, PolicyRegistry, Profile, TaskHints, ThreadPolicy

from .prediction import (HardwareCapabilities, ExecutionObjective, RuntimeSample,
                         WorkloadProfile, ScalingBucket, Prediction, Predictor)

__version__ = "0.3.0"
__all__ = ["ConceptRegistry", "IntelligentExecutor", "BackpressureError", "TaskHints", "Profile", "Policy", "PolicyRegistry", "AdaptivePolicy", "HintPolicy", "ThreadPolicy", "HardwareCapabilities", "ExecutionObjective", "RuntimeSample",
           "WorkloadProfile", "ScalingBucket", "Prediction", "Predictor"]
