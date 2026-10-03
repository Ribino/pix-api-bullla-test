package com.bullla.pix.worker.domain.gateway.pix;

import com.bullla.pix.worker.domain.model.pix.PixTransaction;

public interface PixPartnerGateway {

    PartnerResult process(PixTransaction transaction);
}
