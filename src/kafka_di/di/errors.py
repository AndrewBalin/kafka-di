class DependencyError(RuntimeError):
    pass


class DependencyCycleError(DependencyError):
    pass


class UnresolvedParameterError(DependencyError):
    pass
