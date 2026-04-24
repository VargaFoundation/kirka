package varga.kirka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import varga.kirka.model.Experiment;
import varga.kirka.model.ModelVersion;
import varga.kirka.model.RegisteredModel;
import varga.kirka.model.Run;
import varga.kirka.repo.ExperimentRepository;
import varga.kirka.repo.ModelRegistryRepository;
import varga.kirka.repo.RunRepository;
import varga.kirka.security.SecurityContextHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * GDPR "right to erasure" implementation. Every entry point:
 *
 * <ul>
 *   <li>Requires an authenticated caller with {@code ROLE_ADMIN} (enforced at the controller
 *       via {@code @PreAuthorize}) — no user should be able to trigger a hard delete through
 *       the regular MLFlow API.</li>
 *   <li>Deletes the primary HBase row(s), cascades into child rows (runs, metric history,
 *       model versions, aliases) and wipes the artifact tree on HDFS.</li>
 *   <li>Leaves a trace in the audit log — the {@link varga.kirka.observability.AuditAspect}
 *       records {@code action=delete} with {@code resource_type=gdpr-<type>} for every call.</li>
 * </ul>
 *
 * <p>A confirmation token is required at the HTTP layer to prevent accidental calls. The
 * token equals the resource identifier: purging experiment {@code exp-42} requires
 * {@code X-Kirka-Confirm-Hard-Delete: exp-42}. This is not a security control — the admin
 * role is — but a cheap way to stop {@code curl ... /hard-delete} from nuking the wrong row.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GdprService {

    private final ExperimentRepository experimentRepository;
    private final RunRepository runRepository;
    private final ModelRegistryRepository modelRegistryRepository;
    private final ArtifactService artifactService;
    private final SecurityContextHelper securityContextHelper;

    /**
     * Erases an experiment and every artefact linked to it: child runs (each with their full
     * metric history) and every artifact file on HDFS. The method is intentionally verbose in
     * its logging — a GDPR erasure leaves no trail once it is done, so the log line is the
     * record of the operator intent.
     */
    public DeletionReport hardDeleteExperiment(String experimentId) throws IOException {
        Experiment experiment = experimentRepository.getExperiment(experimentId);
        if (experiment == null) {
            throw new ResourceNotFoundException("Experiment", experimentId);
        }
        Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                experiment.getTags(),
                varga.kirka.model.ExperimentTag::getKey,
                varga.kirka.model.ExperimentTag::getValue);
        securityContextHelper.checkDeleteAccess("experiment", experimentId, experiment.getOwner(), tagsMap);

        List<String> runIds = runRepository.findRunIdsForExperiment(experimentId);
        int artifactsDeleted = 0;
        int runsDeleted = 0;
        for (String runId : runIds) {
            artifactsDeleted += hardDeleteRunInternal(runId, false);
            runsDeleted++;
        }
        experimentRepository.hardDeleteExperiment(experimentId);

        // Wipe the artifact root for the experiment if the service stored one
        String artifactRoot = experiment.getArtifactLocation();
        if (artifactRoot != null && !artifactRoot.isBlank()) {
            try {
                artifactService.deleteArtifact(artifactRoot);
                artifactsDeleted++;
            } catch (IOException e) {
                // The experiment row is already gone; don't rollback on an HDFS glitch.
                log.warn("Experiment {} purged from HBase but artifact root {} could not be removed: {}",
                        experimentId, artifactRoot, e.toString());
            }
        }

        log.warn("GDPR hard-delete: experiment={} runs={} artifactsDeleted={}",
                experimentId, runsDeleted, artifactsDeleted);
        return new DeletionReport("experiment", experimentId, runsDeleted, artifactsDeleted);
    }

    public DeletionReport hardDeleteRun(String runId) throws IOException {
        Run run = runRepository.getRun(runId);
        if (run == null) {
            throw new ResourceNotFoundException("Run", runId);
        }
        Map<String, String> tagsMap = runTagsAsMap(run);
        securityContextHelper.checkDeleteAccess("run", runId,
                run.getInfo() != null ? run.getInfo().getUserId() : null, tagsMap);
        int artifactsDeleted = hardDeleteRunInternal(runId, true);
        log.warn("GDPR hard-delete: run={} artifactsDeleted={}", runId, artifactsDeleted);
        return new DeletionReport("run", runId, 0, artifactsDeleted);
    }

    /** Returns the number of artefact paths removed so the caller can report it. */
    private int hardDeleteRunInternal(String runId, boolean enforceAuthz) throws IOException {
        Run run = runRepository.getRun(runId);
        if (run == null) return 0;
        if (enforceAuthz) {
            securityContextHelper.checkDeleteAccess("run", runId,
                    run.getInfo() != null ? run.getInfo().getUserId() : null, runTagsAsMap(run));
        }
        int deleted = 0;
        String artifactUri = run.getInfo() != null ? run.getInfo().getArtifactUri() : null;
        if (artifactUri != null && !artifactUri.isBlank()) {
            try {
                artifactService.deleteArtifact(artifactUri);
                deleted = 1;
            } catch (IOException e) {
                log.warn("Artifact root {} could not be removed for run {}: {}", artifactUri, runId, e.toString());
            }
        }
        runRepository.hardDeleteRun(runId);
        return deleted;
    }

    public DeletionReport hardDeleteRegisteredModel(String name) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = modelTagsAsMap(model);
        securityContextHelper.checkDeleteAccess("model", name, model.getUserId(), tagsMap);

        int versionsDeleted = 0;
        if (model.getLatestVersions() != null) {
            for (ModelVersion v : model.getLatestVersions()) {
                modelRegistryRepository.deleteModelVersion(v.getName(), v.getVersion());
                versionsDeleted++;
            }
        }
        modelRegistryRepository.deleteRegisteredModel(name);
        log.warn("GDPR hard-delete: model={} versionsDeleted={}", name, versionsDeleted);
        return new DeletionReport("registered-model", name, versionsDeleted, 0);
    }

    private static Map<String, String> runTagsAsMap(Run run) {
        if (run == null || run.getData() == null || run.getData().getTags() == null) return Map.of();
        Map<String, String> out = new java.util.HashMap<>();
        run.getData().getTags().forEach(t -> out.put(t.getKey(), t.getValue()));
        return out;
    }

    private static Map<String, String> modelTagsAsMap(RegisteredModel m) {
        if (m.getTags() == null) return Map.of();
        Map<String, String> out = new java.util.HashMap<>();
        m.getTags().forEach(t -> out.put(t.getKey(), t.getValue()));
        return out;
    }

    /** Immutable record returned by the hard-delete API so operators can log the outcome. */
    public record DeletionReport(String resourceType, String resourceId,
                                 int childRowsDeleted, int artifactPathsDeleted) {}
}
