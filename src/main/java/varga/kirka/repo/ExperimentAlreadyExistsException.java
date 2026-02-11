package varga.kirka.repo;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Exception levée quand une expérience avec le même nom existe déjà.
 */
@ResponseStatus(HttpStatus.CONFLICT)
public class ExperimentAlreadyExistsException extends RuntimeException {

    private final String experimentName;

    public ExperimentAlreadyExistsException(String experimentName) {
        super(String.format("Experiment with name '%s' already exists", experimentName));
        this.experimentName = experimentName;
    }

    public String getExperimentName() {
        return experimentName;
    }
}
