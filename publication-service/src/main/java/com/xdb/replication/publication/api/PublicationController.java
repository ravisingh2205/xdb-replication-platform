package com.xdb.replication.publication.api;

import com.xdb.replication.publication.api.Dtos.PublicationRequest;
import com.xdb.replication.publication.api.Dtos.PublicationResponse;
import com.xdb.replication.publication.api.Dtos.PublicationUpdateRequest;
import com.xdb.replication.publication.connect.ConnectorStatus;
import com.xdb.replication.publication.service.PublicationService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/publications")
public class PublicationController {

    private final PublicationService service;

    public PublicationController(PublicationService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public PublicationResponse create(@Valid @RequestBody PublicationRequest req) {
        return PublicationResponse.from(service.create(req));
    }

    @GetMapping
    public List<PublicationResponse> list() {
        return service.list().stream().map(PublicationResponse::from).toList();
    }

    @GetMapping("/{name}")
    public PublicationResponse get(@PathVariable String name) {
        return PublicationResponse.from(service.get(name));
    }

    @PutMapping("/{name}")
    public PublicationResponse update(@PathVariable String name, @Valid @RequestBody PublicationUpdateRequest req) {
        return PublicationResponse.from(service.update(name, req));
    }

    /** Live Debezium connector + task state from Kafka Connect. */
    @GetMapping("/{name}/connector")
    public ResponseEntity<ConnectorStatus> connector(@PathVariable String name) {
        return ResponseEntity.of(service.connectorStatus(name));
    }

    /** The generated Debezium config (password masked), useful for troubleshooting. */
    @GetMapping("/{name}/connector-config")
    public Map<String, String> connectorConfig(@PathVariable String name) {
        return service.connectorConfig(name);
    }

    @PostMapping("/{name}/pause")
    public PublicationResponse pause(@PathVariable String name) {
        return PublicationResponse.from(service.pause(name));
    }

    @PostMapping("/{name}/resume")
    public PublicationResponse resume(@PathVariable String name) {
        return PublicationResponse.from(service.resume(name));
    }

    @PostMapping("/{name}/restart")
    public PublicationResponse restart(@PathVariable String name) {
        return PublicationResponse.from(service.restart(name));
    }

    @DeleteMapping("/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String name) {
        service.delete(name);
    }
}
