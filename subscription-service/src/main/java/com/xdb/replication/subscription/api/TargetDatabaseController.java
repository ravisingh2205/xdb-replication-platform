package com.xdb.replication.subscription.api;

import com.xdb.replication.subscription.api.Dtos.TargetDatabaseRequest;
import com.xdb.replication.subscription.api.Dtos.TargetDatabaseResponse;
import com.xdb.replication.subscription.service.TargetDatabaseService;
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
@RequestMapping("/api/target-databases")
public class TargetDatabaseController {

    private final TargetDatabaseService service;

    public TargetDatabaseController(TargetDatabaseService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public TargetDatabaseResponse create(@Valid @RequestBody TargetDatabaseRequest req) {
        return TargetDatabaseResponse.from(service.create(req));
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
    public List<TargetDatabaseResponse> list() {
        return service.list().stream().map(TargetDatabaseResponse::from).toList();
    }

    @GetMapping("/{name}")
    public TargetDatabaseResponse get(@PathVariable String name) {
        return TargetDatabaseResponse.from(service.get(name));
    }

    @DeleteMapping("/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String name) {
        service.delete(name);
    }
}
