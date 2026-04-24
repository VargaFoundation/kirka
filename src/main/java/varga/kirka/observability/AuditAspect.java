package varga.kirka.observability;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;
import varga.kirka.service.AuditService;

/**
 * Emits one audit event per successful — or failed — mutating service call.
 *
 * <p>Pointcut: public {@code create*}, {@code update*}, {@code delete*}, {@code restore*},
 * {@code transition*}, {@code log*}, {@code set*} methods on any {@code *Service} class in
 * {@code varga.kirka.service}, except {@link AuditService} itself (to avoid recursion).
 *
 * <p>The aspect extracts the resource type from the service class name (e.g. {@code
 * ExperimentService} → {@code experiment}) and the resource identifier from the first string
 * argument — a convention that holds across the codebase: every write operation takes the
 * target id first. Edge cases (no string argument, method name doesn't fit a verb) are
 * handled gracefully with {@code resourceId=null} so the event is still recorded.
 */
@Slf4j
@Aspect
@Component
public class AuditAspect {

    private final AuditService auditService;

    public AuditAspect(AuditService auditService) {
        this.auditService = auditService;
    }

    @Around(
            "(execution(public * varga.kirka.service..*Service.create*(..)) ||"
          + " execution(public * varga.kirka.service..*Service.update*(..)) ||"
          + " execution(public * varga.kirka.service..*Service.delete*(..)) ||"
          + " execution(public * varga.kirka.service..*Service.restore*(..)) ||"
          + " execution(public * varga.kirka.service..*Service.transition*(..)) ||"
          + " execution(public * varga.kirka.service..*Service.log*(..)) ||"
          + " execution(public * varga.kirka.service..*Service.set*(..)) ||"
          + " execution(public * varga.kirka.service..*Service.rename*(..)))"
          + " && !within(varga.kirka.service.AuditService)"
    )
    public Object audit(ProceedingJoinPoint pjp) throws Throwable {
        String action = verb(pjp.getSignature().getName());
        String resourceType = resourceTypeFromClass(pjp.getSignature().getDeclaringType().getSimpleName());
        String resourceId = firstStringArg(pjp.getArgs());

        try {
            Object result = pjp.proceed();
            auditService.record(action, resourceType, resourceId, "allowed", null);
            return result;
        } catch (Throwable t) {
            // Keep the class name of the exception as a stable, low-cardinality reason.
            String reason = t.getClass().getSimpleName()
                    + (t.getMessage() != null ? ": " + truncate(t.getMessage()) : "");
            String outcome = isAuthzFailure(t) ? "denied" : "error";
            auditService.record(action, resourceType, resourceId, outcome, reason);
            throw t;
        }
    }

    private static String verb(String methodName) {
        String m = methodName.toLowerCase();
        if (m.startsWith("create")) return "create";
        if (m.startsWith("update")) return "update";
        if (m.startsWith("delete")) return "delete";
        if (m.startsWith("restore")) return "restore";
        if (m.startsWith("transition")) return "transition";
        if (m.startsWith("rename")) return "rename";
        if (m.startsWith("log")) return "log";
        if (m.startsWith("set")) return "set";
        return methodName; // fall-back, shouldn't happen given the pointcut
    }

    private static String resourceTypeFromClass(String className) {
        // "ExperimentService" -> "experiment", "ModelRegistryService" -> "model-registry"
        if (className == null) return null;
        String trimmed = className.endsWith("Service")
                ? className.substring(0, className.length() - "Service".length())
                : className;
        // CamelCase -> kebab-case with a single pass
        StringBuilder out = new StringBuilder(trimmed.length() + 4);
        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if (Character.isUpperCase(c) && i > 0) out.append('-');
            out.append(Character.toLowerCase(c));
        }
        return out.toString();
    }

    private static String firstStringArg(Object[] args) {
        if (args == null) return null;
        for (Object a : args) {
            if (a instanceof String s && !s.isBlank()) return s;
        }
        return null;
    }

    private static boolean isAuthzFailure(Throwable t) {
        String name = t.getClass().getSimpleName();
        return "AccessDeniedException".equals(name)
                || "PermissionDeniedException".equals(name);
    }

    private static String truncate(String s) {
        return s.length() > 500 ? s.substring(0, 497) + "..." : s;
    }
}
