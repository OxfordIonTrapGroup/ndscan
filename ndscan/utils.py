from typing import List

def path_matches_spec(path: List[str], spec: str):
    # TODO: Think about how we want to match.
    if spec == "*":
    	return True
    if "*" in spec:
    	raise NotImplementedError("Non-trivial wildcard path specifications not implemented yet")
    return "/".join(path) == spec

def strip_prefix(string: str, prefix: str):
	if string.startswith(prefix):
		return string[len(prefix):]
	return string