from dataclasses import dataclass, field 

@dataclass
class ProcessableFileTypes:
  processable_mimetypes: set[str] = field(default_factory=lambda: {
      'text/plain'
    })
