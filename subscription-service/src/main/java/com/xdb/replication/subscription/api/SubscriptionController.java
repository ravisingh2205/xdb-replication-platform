package com.xdb.replication.subscription.api;

import com.xdb.replication.subscription.api.Dtos.SubscriptionRequest;
import com.xdb.replication.subscription.api.Dtos.SubscriptionResponse;
import com.xdb.replication.subscription.api.Dtos.SubscriptionUpdateRequest;
import com.xdb.replication.subscription.service.SubscriptionService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
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

@RestController
@RequestMapping("/api/subscriptions")
public class SubscriptionController {

    private final SubscriptionService service;

    public SubscriptionController(SubscriptionService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public SubscriptionResponse create(@Valid @RequestBody SubscriptionRequest req) {
        return SubscriptionResponse.from(service.create(req));
    }

    @GetMapping
    public List<SubscriptionResponse> list() {
        return service.list().stream().map(SubscriptionResponse::from).toList();
    }

    @GetMapping("/{name}")
    public SubscriptionResponse get(@PathVariable String name) {
        return SubscriptionResponse.from(service.get(name));
    }

    @PutMapping("/{name}")
    public SubscriptionResponse update(@PathVariable String name, @Valid @RequestBody SubscriptionUpdateRequest req) {
        return SubscriptionResponse.from(service.updateMappings(name, req.tableMappings()));
    }

    @PostMapping("/{name}/pause")
    public SubscriptionResponse pause(@PathVariable String name) {
        return SubscriptionResponse.from(service.setActive(name, false));
    }

    @PostMapping("/{name}/resume")
    public SubscriptionResponse resume(@PathVariable String name) {
        return SubscriptionResponse.from(service.setActive(name, true));
    }

    @DeleteMapping("/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String name) {
        service.delete(name);
    }
}
