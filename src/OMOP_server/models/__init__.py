"""
OMOP CDM Models
"""

from .basetable import ParentBase
from .person_id import Person_ID

# Try to import models, handle missing dependencies gracefully
try:
    from .person import Person
    from .visit_occurrence import VisitOccurrence
    from .condition_occurrence import ConditionOccurrence
    from .procedure_occurrence import ProcedureOccurrence
    from .observation_period import ObservationPeriod
    from .concept_builder import ConceptBuilder
    from .concept_relationship import ConceptRelationship
    
    __all__ = [
        "ParentBase",
        "Person_ID",
        "Person",
        "VisitOccurrence",
        "ConditionOccurrence", 
        "ProcedureOccurrence",
        "ObservationPeriod",
        "ConceptBuilder",
        "ConceptRelationship"
    ]
except ImportError as e:
    print(f"Warning: Some models could not be imported: {e}")
    __all__ = ["ParentBase", "Person_ID"]