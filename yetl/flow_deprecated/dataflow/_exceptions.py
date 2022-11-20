class SourceNotFound(Exception):
    def __init__(self, source_name: str, sources: dict):
        self.source_name = source_name
        self.sources = sources
        available_sources = ",".join(sources.keys())
        msg = f"Source {self.source_name} cannot be found, available sources are {available_sources}"

        super().__init__(msg)


class DestinationNotFound(Exception):
    def __init__(self, destination_name: str, destinations: dict):
        self.destination_name = destination_name
        self.destinations = destinations
        available_sources = ",".join(destinations.keys())
        msg = f"Destination {self.destination_name} cannot be found, available Destinations are {available_sources}"

        super().__init__(msg)
