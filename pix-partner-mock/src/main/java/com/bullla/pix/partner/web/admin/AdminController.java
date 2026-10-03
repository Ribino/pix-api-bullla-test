package com.bullla.pix.partner.web.admin;

import com.bullla.pix.partner.service.PartnerMockService;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/partner/admin")
public class AdminController {

    private final PartnerMockService partnerMockService;

    public AdminController(PartnerMockService partnerMockService) {
        this.partnerMockService = partnerMockService;
    }

    @PostMapping("/scenario")
    public ResponseEntity<Void> scenario(@RequestBody ScenarioRequest request) {
        partnerMockService.configure(
                request.mode(), request.latencyMs(), request.failTimes(), request.slowDelayMs());
        return ResponseEntity.ok().build();
    }

    @PostMapping("/reset")
    public ResponseEntity<Void> reset() {
        partnerMockService.reset();
        return ResponseEntity.ok().build();
    }

    @GetMapping("/stats")
    public ResponseEntity<PartnerMockService.MockStats> stats() {
        return ResponseEntity.ok(partnerMockService.stats());
    }
}
