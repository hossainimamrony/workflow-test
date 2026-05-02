from dataclasses import dataclass


@dataclass(frozen=True)
class WorkflowCard:
    title: str
    description: str
    path: str
