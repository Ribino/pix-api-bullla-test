package com.bullla.pix.partner.web;

import com.bullla.pix.partner.service.PartnerMockService;

import jakarta.validation.Valid;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/partner/pix")
public class PartnerController {

    private final PartnerMockService partnerMockService;

    public PartnerController(PartnerMockService partnerMockService) {
        this.partnerMockService = partnerMockService;
    }

    @PostMapping
    public ResponseEntity<PartnerResponse> process(@Valid @RequestBody PartnerRequest request) {
        return partnerMockService.process(request);
    }
}
