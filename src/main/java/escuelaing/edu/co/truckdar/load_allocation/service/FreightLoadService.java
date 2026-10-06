package escuelaing.edu.co.truckdar.load_allocation.service;

import escuelaing.edu.co.truckdar.load_allocation.dto.event.FreightLoadPublishedEvent;
import escuelaing.edu.co.truckdar.load_allocation.dto.request.AssignDriverRequest;
import escuelaing.edu.co.truckdar.load_allocation.dto.request.CreateFreightLoadRequest;
import escuelaing.edu.co.truckdar.load_allocation.dto.response.FreightLoadResponse;
import escuelaing.edu.co.truckdar.load_allocation.exception.FreightLoadNotFoundException;
import escuelaing.edu.co.truckdar.load_allocation.exception.InvalidLoadOperationException;
import escuelaing.edu.co.truckdar.load_allocation.model.FreightLoad;
import escuelaing.edu.co.truckdar.load_allocation.model.LoadStatus;
import escuelaing.edu.co.truckdar.load_allocation.model.TruckTypeRequired;
import escuelaing.edu.co.truckdar.load_allocation.repository.FreightLoadRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class FreightLoadService implements IFreightLoadService {

    private final FreightLoadRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${truckdar.kafka.topic.load-events:truckdar.events.load-allocation}")
    private String loadEventsTopic;

    @Override
    @Transactional
    public FreightLoadResponse publishLoad(CreateFreightLoadRequest request) {
        FreightLoad load = FreightLoad.builder()
                .generatorCompanyId(request.getGeneratorCompanyId())
                .generatorCompanyName(request.getGeneratorCompanyName())
                .cargoDescription(request.getCargoDescription())
                .weightTons(request.getWeightTons())
                .volumeM3(request.getVolumeM3())
                .requiredTruckType(request.getRequiredTruckType())
                .originCity(request.getOriginCity())
                .originDepartment(request.getOriginDepartment())
                .originLatitude(request.getOriginLatitude())
                .originLongitude(request.getOriginLongitude())
                .destinationCity(request.getDestinationCity())
                .destinationDepartment(request.getDestinationDepartment())
                .destinationLatitude(request.getDestinationLatitude())
                .destinationLongitude(request.getDestinationLongitude())
                .offeredBudget(request.getOfferedBudget())
                .status(LoadStatus.AVAILABLE)
                .publishedAt(LocalDateTime.now())
                .build();

        FreightLoad saved = repository.save(load);
        publishKafkaEvent(saved, "PUBLISHED");
        log.info("Carga publicada #{} por {} ({} Tn)", saved.getId(), saved.getGeneratorCompanyName(), saved.getWeightTons());

        return mapToResponse(saved);
    }

    @Override
    @Transactional
    public FreightLoadResponse assignDriver(Long loadId, AssignDriverRequest request) {
        FreightLoad load = repository.findById(loadId)
                .orElseThrow(() -> new FreightLoadNotFoundException("No se encontró la carga con ID: " + loadId));

        if (load.getStatus() != LoadStatus.AVAILABLE) {
            throw new InvalidLoadOperationException("La carga #" + loadId + " no está disponible para asignación (estado actual: " + load.getStatus() + ")");
        }

        load.setAssignedDriverId(request.getDriverId());
        load.setAssignedDriverName(request.getDriverName());
        load.setAssignedVehiclePlate(request.getVehiclePlate());
        load.setStatus(LoadStatus.ASSIGNED);
        load.setAssignedAt(LocalDateTime.now());

        FreightLoad updated = repository.save(load);
        publishKafkaEvent(updated, "ASSIGNED");
        log.info("Carga #{} asignada al conductor {} (placa: {})", updated.getId(), request.getDriverName(), request.getVehiclePlate());

        return mapToResponse(updated);
    }

    @Override
    @Transactional(readOnly = true)
    public FreightLoadResponse getLoadById(Long loadId) {
        return repository.findById(loadId)
                .map(this::mapToResponse)
                .orElseThrow(() -> new FreightLoadNotFoundException("No se encontró la carga con ID: " + loadId));
    }

    @Override
    @Transactional(readOnly = true)
    public List<FreightLoadResponse> getAvailableLoads(TruckTypeRequired truckType) {
        List<FreightLoad> results = (truckType != null)
                ? repository.findByStatusAndRequiredTruckType(LoadStatus.AVAILABLE, truckType)
                : repository.findByStatusOrderByPublishedAtDesc(LoadStatus.AVAILABLE);

        return results.stream().map(this::mapToResponse).collect(Collectors.toList());
    }

    @Override
    @Transactional(readOnly = true)
    public List<FreightLoadResponse> getLoadsByCompany(String generatorCompanyId) {
        return repository.findByGeneratorCompanyIdOrderByPublishedAtDesc(generatorCompanyId)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    private void publishKafkaEvent(FreightLoad load, String eventType) {
        FreightLoadPublishedEvent event = FreightLoadPublishedEvent.builder()
                .loadId(load.getId())
                .generatorCompanyId(load.getGeneratorCompanyId())
                .cargoDescription(load.getCargoDescription())
                .weightTons(load.getWeightTons())
                .requiredTruckType(load.getRequiredTruckType().name())
                .originCity(load.getOriginCity())
                .destinationCity(load.getDestinationCity())
                .offeredBudget(load.getOfferedBudget())
                .eventType(eventType)
                .timestamp(LocalDateTime.now())
                .build();

        kafkaTemplate.send(loadEventsTopic, load.getId().toString(), event);
    }

    private FreightLoadResponse mapToResponse(FreightLoad entity) {
        return FreightLoadResponse.builder()
                .id(entity.getId())
                .generatorCompanyId(entity.getGeneratorCompanyId())
                .generatorCompanyName(entity.getGeneratorCompanyName())
                .cargoDescription(entity.getCargoDescription())
                .weightTons(entity.getWeightTons())
                .volumeM3(entity.getVolumeM3())
                .requiredTruckType(entity.getRequiredTruckType())
                .originCity(entity.getOriginCity())
                .originDepartment(entity.getOriginDepartment())
                .originLatitude(entity.getOriginLatitude())
                .originLongitude(entity.getOriginLongitude())
                .destinationCity(entity.getDestinationCity())
                .destinationDepartment(entity.getDestinationDepartment())
                .destinationLatitude(entity.getDestinationLatitude())
                .destinationLongitude(entity.getDestinationLongitude())
                .offeredBudget(entity.getOfferedBudget())
                .status(entity.getStatus())
                .assignedDriverId(entity.getAssignedDriverId())
                .assignedDriverName(entity.getAssignedDriverName())
                .assignedVehiclePlate(entity.getAssignedVehiclePlate())
                .publishedAt(entity.getPublishedAt())
                .assignedAt(entity.getAssignedAt())
                .build();
    }
}