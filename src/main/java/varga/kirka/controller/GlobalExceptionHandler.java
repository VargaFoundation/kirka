package varga.kirka.controller;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import varga.kirka.repo.ExperimentAlreadyExistsException;
import varga.kirka.security.AccessDeniedException;
import varga.kirka.service.ResourceNotFoundException;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final String MDC_REQUEST_ID = "request_id";

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleResourceNotFound(ResourceNotFoundException ex) {
        log.warn("Resource not found: {}", ex.getMessage());
        return mlflowError(HttpStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", ex.getMessage());
    }

    @ExceptionHandler(ExperimentAlreadyExistsException.class)
    public ResponseEntity<Map<String, Object>> handleAlreadyExists(ExperimentAlreadyExistsException ex) {
        log.warn("Resource already exists: {}", ex.getMessage());
        return mlflowError(HttpStatus.CONFLICT, "RESOURCE_ALREADY_EXISTS", ex.getMessage());
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<Map<String, Object>> handleAccessDenied(AccessDeniedException ex) {
        log.warn("Access denied: {}", ex.getMessage());
        return mlflowError(HttpStatus.FORBIDDEN, "PERMISSION_DENIED", ex.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException ex) {
        log.warn("Bad request: {}", ex.getMessage());
        return mlflowError(HttpStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", ex.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleBeanValidation(MethodArgumentNotValidException ex) {
        String details = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + " " + fe.getDefaultMessage())
                .collect(Collectors.joining("; "));
        log.warn("Validation failed: {}", details);
        return mlflowError(HttpStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", details);
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<Map<String, Object>> handleMissingParam(MissingServletRequestParameterException ex) {
        log.warn("Missing request parameter: {}", ex.getParameterName());
        return mlflowError(HttpStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE",
                "Missing required parameter: " + ex.getParameterName());
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleUnreadable(HttpMessageNotReadableException ex) {
        log.warn("Malformed request body: {}", ex.getMostSpecificCause().getMessage());
        return mlflowError(HttpStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE",
                "Request body is malformed or does not match the expected schema");
    }

    @ExceptionHandler(IOException.class)
    public ResponseEntity<Map<String, Object>> handleIOException(IOException ex) {
        log.error("Internal storage error", ex);
        return mlflowError(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
                "Storage operation failed");
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleAny(Exception ex) {
        // Catch-all so that unhandled exceptions do not leak stack traces to the client.
        log.error("Unhandled exception", ex);
        return mlflowError(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
                "An unexpected error occurred");
    }

    private static ResponseEntity<Map<String, Object>> mlflowError(HttpStatus status, String code, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error_code", code);
        body.put("message", message);
        String requestId = MDC.get(MDC_REQUEST_ID);
        if (requestId != null && !requestId.isEmpty()) {
            body.put("request_id", requestId);
        }
        return ResponseEntity.status(status).body(body);
    }
}
