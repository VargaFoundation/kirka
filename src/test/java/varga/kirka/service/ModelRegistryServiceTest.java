package varga.kirka.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import varga.kirka.model.ModelVersion;
import varga.kirka.model.RegisteredModel;
import varga.kirka.repo.ModelRegistryRepository;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ModelRegistryServiceTest {

    @Mock
    private ModelRegistryRepository modelRegistryRepository;

    @InjectMocks
    private ModelRegistryService modelRegistryService;

    @Test
    public void testCreateRegisteredModel() throws IOException {
        String name = "test-model";
        modelRegistryService.createRegisteredModel(name);
        verify(modelRegistryRepository, times(1)).createRegisteredModel(name);
    }

    @Test
    public void testCreateModelVersion() throws IOException {
        String name = "test-model";
        RegisteredModel model = RegisteredModel.builder().name(name).latestVersions(Collections.emptyList()).build();
        when(modelRegistryRepository.getRegisteredModel(name)).thenReturn(model);
        
        ModelVersion version = modelRegistryService.createModelVersion(name, "source", "run123");
        
        assertEquals("1", version.getVersion());
        assertEquals("None", version.getCurrentStage());
        verify(modelRegistryRepository, times(1)).createModelVersion(any(ModelVersion.class));
    }

    @Test
    public void testTransitionModelVersionStage() throws IOException {
        String name = "test-model";
        String version = "1";
        ModelVersion mvProduction = ModelVersion.builder().name(name).version(version).currentStage("Production").build();
        
        when(modelRegistryRepository.getModelVersion(name, version)).thenReturn(mvProduction);

        ModelVersion result = modelRegistryService.transitionModelVersionStage(name, version, "Production", false);
        
        assertEquals("Production", result.getCurrentStage());
        verify(modelRegistryRepository).updateModelVersionStage(name, version, "Production");
    }
}