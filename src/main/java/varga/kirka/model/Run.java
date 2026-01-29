package varga.kirka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Run {
    private RunInfo info;
    private RunData data;
    private RunInputs inputs;
    private RunOutputs outputs;
}
