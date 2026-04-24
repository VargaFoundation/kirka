package varga.kirka.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import varga.kirka.model.ModelVersion;
import varga.kirka.model.RegisteredModel;
import varga.kirka.repo.ModelRegistryRepository;
import varga.kirka.security.SecurityContextHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ModelRegistryServiceTest {

    @Mock
    private ModelRegistryRepository modelRegistryRepository;

    @Mock
    private SecurityContextHelper securityContextHelper;

    @InjectMocks
    private ModelRegistryService modelRegistryService;

    @BeforeEach
    void setUpAuthz() {
        when(securityContextHelper.getCurrentUser()).thenReturn("alice");
        when(securityContextHelper.tagsToMap(any(), any(), any())).thenReturn(Map.of());
        when(securityContextHelper.canRead(any(), any(), any(), any())).thenReturn(true);
    }

    private RegisteredModel existingModel(String name) {
        return RegisteredModel.builder().name(name).latestVersions(Collections.emptyList()).build();
    }

    @Test
    public void testCreateRegisteredModel() throws IOException {
        modelRegistryService.createRegisteredModel("test-model");
        verify(modelRegistryRepository, times(1)).createRegisteredModel("test-model");
    }

    @Test
    public void testCreateModelVersion() throws IOException {
        when(modelRegistryRepository.getRegisteredModel("test-model")).thenReturn(existingModel("test-model"));

        ModelVersion version = modelRegistryService.createModelVersion("test-model", "source", "run123");

        assertEquals("1", version.getVersion());
        assertEquals("None", version.getCurrentStage());
        verify(modelRegistryRepository, times(1)).createModelVersion(any(ModelVersion.class));
    }

    @Test
    public void testTransitionModelVersionStage() throws IOException {
        ModelVersion mvProduction = ModelVersion.builder().name("test-model").version("1").currentStage("Production").build();
        when(modelRegistryRepository.getRegisteredModel("test-model")).thenReturn(existingModel("test-model"));
        when(modelRegistryRepository.getModelVersion("test-model", "1")).thenReturn(mvProduction);

        ModelVersion result = modelRegistryService.transitionModelVersionStage("test-model", "1", "Production", false);

        assertEquals("Production", result.getCurrentStage());
        verify(modelRegistryRepository).updateModelVersionStage("test-model", "1", "Production");
    }

    @Test
    public void testSearchRegisteredModels() throws IOException {
        RegisteredModel m1 = RegisteredModel.builder().name("model1").build();
        RegisteredModel m2 = RegisteredModel.builder().name("model2").build();
        when(modelRegistryRepository.listRegisteredModels()).thenReturn(List.of(m1, m2));

        List<RegisteredModel> results = modelRegistryService.searchRegisteredModels("name LIKE 'model1%'");
        assertEquals(1, results.size());
        assertEquals("model1", results.get(0).getName());
    }

    @Test
    public void testUpdateDeleteRegisteredModel() throws IOException {
        when(modelRegistryRepository.getRegisteredModel("m1")).thenReturn(existingModel("m1"));

        modelRegistryService.updateRegisteredModel("m1", "desc");
        verify(modelRegistryRepository).updateRegisteredModel("m1", "desc");

        modelRegistryService.deleteRegisteredModel("m1");
        verify(modelRegistryRepository).deleteRegisteredModel("m1");
    }

    @Test
    public void testUpdateDeleteModelVersion() throws IOException {
        when(modelRegistryRepository.getRegisteredModel("m1")).thenReturn(existingModel("m1"));

        modelRegistryService.updateModelVersion("m1", "1", "desc");
        verify(modelRegistryRepository).updateModelVersion("m1", "1", "desc");

        modelRegistryService.deleteModelVersion("m1", "1");
        verify(modelRegistryRepository).deleteModelVersion("m1", "1");
    }

    @Test
    public void testModelTags() throws IOException {
        when(modelRegistryRepository.getRegisteredModel("m1")).thenReturn(existingModel("m1"));

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
