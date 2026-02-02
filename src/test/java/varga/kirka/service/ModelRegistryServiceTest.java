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
import java.util.List;

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

    @Test
    public void testSearchRegisteredModels() throws IOException {
        RegisteredModel m1 = RegisteredModel.builder().name("model1").build();
        RegisteredModel m2 = RegisteredModel.builder().name("model2").build();
        when(modelRegistryRepository.listRegisteredModels()).thenReturn(List.of(m1, m2));

        java.util.List<RegisteredModel> results = modelRegistryService.searchRegisteredModels("name LIKE 'model1%'");
        assertEquals(1, results.size());
        assertEquals("model1", results.get(0).getName());
    }

    @Test
    public void testUpdateDeleteRegisteredModel() throws IOException {
        modelRegistryService.updateRegisteredModel("m1", "desc");
        verify(modelRegistryRepository).updateRegisteredModel("m1", "desc");

        modelRegistryService.deleteRegisteredModel("m1");
        verify(modelRegistryRepository).deleteRegisteredModel("m1");
    }

    @Test
    public void testUpdateDeleteModelVersion() throws IOException {
        modelRegistryService.updateModelVersion("m1", "1", "desc");
        verify(modelRegistryRepository).updateModelVersion("m1", "1", "desc");

        modelRegistryService.deleteModelVersion("m1", "1");
        verify(modelRegistryRepository).deleteModelVersion("m1", "1");
    }

    @Test
    public void testModelTags() throws IOException {
        modelRegistryService.setRegisteredModelTag("m1", "k", "v");
        verify(modelRegistryRepository).setRegisteredModelTag("m1", "k", "v");

        modelRegistryService.deleteRegisteredModelTag("m1", "k");
        verify(modelRegistryRepository).deleteRegisteredModelTag("m1", "k");

        modelRegistryService.setModelVersionTag("m1", "1", "k", "v");
        verify(modelRegistryRepository).setModelVersionTag("m1", "1", "k", "v");

        modelRegistryService.deleteModelVersionTag("m1", "1", "k");
        verify(modelRegistryRepository).deleteModelVersionTag("m1", "1", "k");
    }
}