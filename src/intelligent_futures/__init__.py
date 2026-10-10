"""A bounded, measured scheduling layer on concurrent.futures."""
from .catalog import ConceptRegistry
from .executor import BackpressureError, IntelligentExecutor
from .policies import AdaptivePolicy, BackendLoad, SchedulingLoad, HintPolicy, Policy, PolicyRegistry, Profile, TaskHints, ThreadPolicy

from .prediction import (HardwareCapabilities, ExecutionObjective, RuntimeSample,
                         WorkloadProfile, ScalingBucket, Prediction, Predictor)

__version__ = "0.4.0"
__all__ = ["BackendLoad", "SchedulingLoad", "ConceptRegistry", "IntelligentExecutor", "BackpressureError", "TaskHints", "Profile", "Policy", "PolicyRegistry", "AdaptivePolicy", "HintPolicy", "ThreadPolicy", "HardwareCapabilities", "ExecutionObjective", "RuntimeSample",
           "WorkloadProfile", "ScalingBucket", "Prediction", "Predictor"]
