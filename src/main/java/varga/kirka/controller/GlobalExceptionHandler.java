package varga.kirka.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import varga.kirka.repo.ExperimentAlreadyExistsException;
import varga.kirka.security.AccessDeniedException;
import varga.kirka.service.ResourceNotFoundException;

import java.io.IOException;
import java.util.Map;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleResourceNotFound(ResourceNotFoundException ex) {
        log.warn("Resource not found: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error_code", "RESOURCE_DOES_NOT_EXIST", "message", ex.getMessage()));
    }

    @ExceptionHandler(ExperimentAlreadyExistsException.class)
    public ResponseEntity<Map<String, Object>> handleAlreadyExists(ExperimentAlreadyExistsException ex) {
        log.warn("Resource already exists: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("error_code", "RESOURCE_ALREADY_EXISTS", "message", ex.getMessage()));
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<Map<String, Object>> handleAccessDenied(AccessDeniedException ex) {
        log.warn("Access denied: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(Map.of("error_code", "PERMISSION_DENIED", "message", ex.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException ex) {
        log.warn("Bad request: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error_code", "INVALID_PARAMETER_VALUE", "message", ex.getMessage()));
    }

    @ExceptionHandler(IOException.class)
    public ResponseEntity<Map<String, Object>> handleIOException(IOException ex) {
        log.error("Internal storage error: {}", ex.getMessage(), ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error_code", "INTERNAL_ERROR", "message", "Storage operation failed: " + ex.getMessage()));
    }
}
