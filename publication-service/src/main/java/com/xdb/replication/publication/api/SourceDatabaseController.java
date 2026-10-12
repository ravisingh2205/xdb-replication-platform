package com.xdb.replication.publication.api;

import com.xdb.replication.publication.api.Dtos.SourceDatabaseRequest;
import com.xdb.replication.publication.api.Dtos.SourceDatabaseResponse;
import com.xdb.replication.publication.service.SourceDatabaseService;
import com.xdb.replication.common.model.JdbcProbe;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/source-databases")
public class SourceDatabaseController {

    private final SourceDatabaseService service;

    public SourceDatabaseController(SourceDatabaseService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public SourceDatabaseResponse create(@Valid @RequestBody SourceDatabaseRequest req) {
        return SourceDatabaseResponse.from(service.create(req));
    }

    /** "Test" button: checks connectivity and credentials without saving anything. */
    @PostMapping("/test")
    public JdbcProbe.Result test(@Valid @RequestBody Dtos.ConnectionTestRequest req) {
        return service.test(req);
    }

    @GetMapping("/{name}/tables")
    public List<String> tables(@PathVariable String name) {
        return service.tables(name);
    }

    @GetMapping
    public List<SourceDatabaseResponse> list() {
        return service.list().stream().map(SourceDatabaseResponse::from).toList();
    }

    @GetMapping("/{name}")
    public SourceDatabaseResponse get(@PathVariable String name) {
        return SourceDatabaseResponse.from(service.get(name));
    }

    @DeleteMapping("/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String name) {
        service.delete(name);
    }
}
