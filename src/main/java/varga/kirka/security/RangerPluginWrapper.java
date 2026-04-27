package varga.kirka.security;

import lombok.extern.slf4j.Slf4j;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.Date;
import java.util.Map;

/**
 * Thin wrapper around Apache Ranger's {@link RangerBasePlugin}.
 *
 * <p>The plugin pulls policies from Ranger Admin (URL configured in
 * {@code ranger-<appId>-security.xml} on the classpath) and evaluates them locally,
 * so an outage of Ranger Admin does not break authorization — the last cached policy set
 * keeps being used until the next successful refresh.
 *
 * <p>When initialisation fails (Ranger Admin unreachable at startup, classpath mismatch,
 * missing credentials, etc.) the wrapper records the failure and reports
 * {@link #isInitialized()} {@code == false}. Callers are expected to fall back to a safer
 * policy (typically owner-only authorization) rather than silently allowing everything.
 *
 * <p>This class used to be a stub with toy string-matching helpers ({@code isTeamMember},
 * {@code isDepartmentMember}). Those have been removed; the real policy engine replaces them.
 */
@Slf4j
public class RangerPluginWrapper {

    private final String serviceType;
    private final String appId;
    private final String policyRestUrl;
    private final String policyCacheDir;

    private RangerBasePlugin plugin;
    private boolean initialized;

    public RangerPluginWrapper(String serviceType, String appId, String policyRestUrl, String policyCacheDir) {
        this.serviceType = serviceType;
        this.appId = appId;
        this.policyRestUrl = policyRestUrl;
        this.policyCacheDir = policyCacheDir;
    }

    /**
     * Initialises the Ranger plugin. Must be called before any {@link #isAccessAllowed} call.
     * On failure the wrapper stays uninitialised and subsequent checks deny by default.
     *
     * <p>Critical Ranger config keys are injected programmatically rather than read from an
     * XML resource on the classpath. The Ranger client looks for {@code ranger-<svc>-security.xml}
     * at the classpath root, but a Spring Boot fat-jar serves resources under
     * {@code BOOT-INF/classes/...} — a path the Ranger resource-loader does not consult. Without
     * the override, {@code policy.rest.url} resolves to {@code null} and {@code RangerRESTClient}
     * crashes on {@code Random.nextInt(0)} because the URL list is empty.
     */
    public void init() {
        log.info("Initialising Ranger plugin: serviceType={}, appId={}, restUrl={}",
                serviceType, appId, policyRestUrl);
        try {
            // Use the 3-arg constructor (serviceType, serviceName, appId) — the 2-arg variant
            // passes serviceName=null to RangerPluginConfig, which later trips a NPE inside
            // RangerAdminRESTClient.init when it URL-encodes the service name.
            RangerBasePlugin newPlugin = new RangerBasePlugin(serviceType, serviceType, appId);
            String prefix = "ranger.plugin." + serviceType;
            if (policyRestUrl != null && !policyRestUrl.isBlank()) {
                newPlugin.getConfig().set(prefix + ".policy.rest.url", policyRestUrl);
            }
            if (policyCacheDir != null && !policyCacheDir.isBlank()) {
                newPlugin.getConfig().set(prefix + ".policy.cache.dir", policyCacheDir);
            }
            newPlugin.init();
            newPlugin.setResultProcessor(new RangerDefaultAuditHandler());
            this.plugin = newPlugin;
            this.initialized = true;
            log.info("Ranger plugin initialised successfully (service={}, cacheDir={})",
                    serviceType,
                    newPlugin.getConfig().get(prefix + ".policy.cache.dir"));
        } catch (Throwable t) {
            this.initialized = false;
            this.plugin = null;
            log.error("Failed to initialise Ranger plugin; authorization will fall back to owner-only checks", t);
        }
    }

    public boolean isInitialized() {
        return initialized && plugin != null;
    }

    /**
     * Delegates to the embedded Ranger policy engine. The request is translated from Kirka's
     * DTO to Ranger's {@link RangerAccessRequestImpl}; the two-level resource tree
     * ({@code resourceType → resourceId}) matches the service-def shipped under
     * {@code src/main/resources/ranger/ranger-servicedef-kirka.json}.
     */
    public boolean isAccessAllowed(RangerAccessRequest request) {
        if (!isInitialized()) {
            log.debug("Ranger plugin not initialised, denying access");
            return false;
        }
        try {
            RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
            // The service-def keys the policy tree by per-type resource names (experiment, run,
            // model, gateway, scorer, prompt). The bucket name drives which resource-type slot
            // the id goes into; unknown types default to "experiment" to stay safe (deny by
            // default on unknown slots).
            String type = request.getResourceType() != null ? request.getResourceType() : "experiment";
            resource.setValue(type, request.getResourceId() != null ? request.getResourceId() : "*");

            RangerAccessRequestImpl rangerRequest = new RangerAccessRequestImpl();
            rangerRequest.setResource(resource);
            rangerRequest.setUser(request.getUser());
            if (request.getUserGroups() != null) {
                rangerRequest.setUserGroups(request.getUserGroups());
            }
            rangerRequest.setAccessType(request.getAccessType() != null ? request.getAccessType() : "read");
            rangerRequest.setClientIPAddress(request.getClientIpAddress());
            rangerRequest.setAccessTime(new Date(
                    request.getAccessTime() > 0 ? request.getAccessTime() : System.currentTimeMillis()));
            rangerRequest.setAction(rangerRequest.getAccessType());

            Map<String, Object> ctx = request.getContext();
            if (ctx != null && !ctx.isEmpty()) {
                rangerRequest.setRequestData(ctx.toString());
            }

            RangerAccessResult result = plugin.isAccessAllowed(rangerRequest);
            boolean allowed = result != null && result.getIsAllowed();
            if (log.isDebugEnabled()) {
                log.debug("Ranger decision user={} type={} id={} access={} -> allowed={}",
                        request.getUser(), type, resource.getValue(type),
                        rangerRequest.getAccessType(), allowed);
            }
            return allowed;
        } catch (Throwable t) {
            log.error("Ranger evaluation failed, denying access", t);
            return false;
        }
    }

    /** Releases the embedded plugin (closes HTTP client, stops policy refresh thread). */
    public void cleanup() {
        if (plugin != null) {
            try {
                plugin.cleanup();
            } catch (Throwable t) {
                log.warn("Error while cleaning up Ranger plugin", t);
            } finally {
                plugin = null;
                initialized = false;
            }
        }
    }
}
