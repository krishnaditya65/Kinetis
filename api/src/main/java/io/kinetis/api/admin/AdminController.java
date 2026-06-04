package io.kinetis.api.admin;

import io.kinetis.core.admin.ArchivalService;
import io.kinetis.core.admin.MaintenanceFlag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

/**
 * Admin operations — maintenance mode and manual archival trigger.
 *
 * <pre>
 *   PUT    /admin/maintenance/on    — pause all scheduler loops
 *   DELETE /admin/maintenance       — resume scheduler loops
 *   GET    /admin/maintenance       — current status
 *   POST   /admin/archive           — trigger one archival batch immediately
 *   GET    /admin/archive           — archived row count
 * </pre>
 *
 * In production, protect these endpoints behind the API key auth filter or firewall rules.
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final MaintenanceFlag maintenanceFlag;
    private final ArchivalService archivalService;

    public AdminController(MaintenanceFlag maintenanceFlag, ArchivalService archivalService) {
        this.maintenanceFlag = maintenanceFlag;
        this.archivalService = archivalService;
    }

    @PostMapping("/maintenance/on")
    public ResponseEntity<Map<String, Object>> enableMaintenance() {
        maintenanceFlag.enable();
        return ResponseEntity.ok(Map.of(
                "maintenance", true,
                "message", "Scheduler loops paused. Disable with DELETE /admin/maintenance.",
                "at", Instant.now()));
    }

    @DeleteMapping("/maintenance")
    public ResponseEntity<Map<String, Object>> disableMaintenance() {
        maintenanceFlag.disable();
        return ResponseEntity.ok(Map.of(
                "maintenance", false,
                "message", "Scheduler loops resumed.",
                "at", Instant.now()));
    }

    @GetMapping("/maintenance")
    public Map<String, Object> maintenanceStatus() {
        return Map.of("maintenance", maintenanceFlag.isEnabled(), "at", Instant.now());
    }

    @PostMapping("/archive")
    public Map<String, Object> triggerArchival() {
        int archived = archivalService.tick();
        return Map.of("archived", archived, "totalArchived", archivalService.archivedCount(),
                "at", Instant.now());
    }

    @GetMapping("/archive")
    public Map<String, Object> archivalStatus() {
        return Map.of("totalArchived", archivalService.archivedCount(), "at", Instant.now());
    }
}
