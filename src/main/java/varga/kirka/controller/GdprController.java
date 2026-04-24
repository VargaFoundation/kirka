package varga.kirka.controller;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import varga.kirka.service.GdprService;

import java.io.IOException;

/**
 * GDPR "right to erasure" endpoint. Everything here is admin-only and never called through
 * the regular MLFlow API — clients that want to free up a name should use the soft-delete
 * endpoints (`/experiments/delete`, `/runs/delete`, …) which preserve an audit trail. This
 * endpoint is a last resort for data subject requests and legal retention cleanups.
 *
 * <p>A confirmation header echoing the resource id is required to stop accidental calls:
 *
 * <pre>
 *   POST /api/2.0/kirka/gdpr/hard-delete
 *   X-Kirka-Confirm-Hard-Delete: exp-42
 *   {"resource_type": "experiment", "resource_id": "exp-42"}
 * </pre>
 */
@Slf4j
@RestController
@RequestMapping("/api/2.0/kirka/gdpr")
@RequiredArgsConstructor
public class GdprController {

    public static final String CONFIRMATION_HEADER = "X-Kirka-Confirm-Hard-Delete";

    private final GdprService gdprService;

    @lombok.Data
    public static class HardDeleteRequest {
        @NotBlank
        @Pattern(regexp = "experiment|run|registered-model",
                 message = "resource_type must be 'experiment', 'run' or 'registered-model'")
        private String resource_type;

        @NotBlank
        private String resource_id;
    }

    @PostMapping("/hard-delete")
    @PreAuthorize("hasRole('ADMIN')")
    public GdprService.DeletionReport hardDelete(@Valid @RequestBody HardDeleteRequest body,
                                                 HttpServletRequest request) throws IOException {
        String confirmation = request.getHeader(CONFIRMATION_HEADER);
        if (confirmation == null || !confirmation.equals(body.getResource_id())) {
            throw new IllegalArgumentException(
                    "Missing or incorrect confirmation header '" + CONFIRMATION_HEADER
                            + "'. The header value must equal resource_id to authorize the hard delete.");
        }

        log.warn("GDPR hard-delete requested: type={} id={}", body.getResource_type(), body.getResource_id());

        return switch (body.getResource_type()) {
            case "experiment" -> gdprService.hardDeleteExperiment(body.getResource_id());
            case "run" -> gdprService.hardDeleteRun(body.getResource_id());
            case "registered-model" -> gdprService.hardDeleteRegisteredModel(body.getResource_id());
            default -> throw new IllegalArgumentException("Unsupported resource_type: " + body.getResource_type());
        };
    }
}
