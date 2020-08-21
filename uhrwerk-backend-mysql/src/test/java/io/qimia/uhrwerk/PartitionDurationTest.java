package io.qimia.uhrwerk;

import io.qimia.uhrwerk.common.model.PartitionTransformType;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

public class PartitionDurationTest {

    @Test
    void identityChecks() {
        var tableDur1 = Duration.ofMinutes(15);
        var depCheck1 = new PartitionDurationTester.PartitionTestDependencyInput();
        depCheck1.dependencyTableName = "check1";
        depCheck1.transformType = PartitionTransformType.IDENTITY;
        depCheck1.dependencyTablePartitionSize = 15;
        depCheck1.dependencyTablePartitionUnit = PartitionUnit.MINUTES;
        assertTrue(PartitionDurationTester.checkDependency(tableDur1, depCheck1));

        var tableDur2 = Duration.ofHours(1);
        assertFalse(PartitionDurationTester.checkDependency(tableDur2, depCheck1));

        var depCheck2 = new PartitionDurationTester.PartitionTestDependencyInput();
        depCheck2.dependencyTableName = "check2";
        depCheck2.transformType = PartitionTransformType.IDENTITY;
        depCheck2.dependencyTablePartitionSize = 1;
        depCheck2.dependencyTablePartitionUnit = PartitionUnit.HOURS;
        assertTrue(PartitionDurationTester.checkDependency(tableDur2, depCheck2));

        var tableDur3 = Duration.ofDays(7 * 3);
        var depCheck3 = new PartitionDurationTester.PartitionTestDependencyInput();
        depCheck3.dependencyTableName = "check3";
        depCheck3.transformType = PartitionTransformType.IDENTITY;
        depCheck3.dependencyTablePartitionSize = 3;
        depCheck3.dependencyTablePartitionUnit = PartitionUnit.WEEKS;
        assertTrue(PartitionDurationTester.checkDependency(tableDur3, depCheck3));
    }

    @Test
    void windowChecks() {
        var tableDur1 = Duration.ofMinutes(20);
        var depCheck1 = new PartitionDurationTester.PartitionTestDependencyInput();
        depCheck1.dependencyTableName = "check1";
        depCheck1.transformType = PartitionTransformType.WINDOW;
        depCheck1.transformSize = 10;
        depCheck1.dependencyTablePartitionSize = 20;
        depCheck1.dependencyTablePartitionUnit = PartitionUnit.MINUTES;
        assertTrue(PartitionDurationTester.checkDependency(tableDur1, depCheck1));
    }

    @Test
    void aggregateChecks() {
        var tableDur1 = Duration.ofMinutes(30);
        var depCheck1 = new PartitionDurationTester.PartitionTestDependencyInput();
        depCheck1.dependencyTableName = "check1";
        depCheck1.transformType = PartitionTransformType.AGGREGATE;
        depCheck1.transformSize = 2;
        depCheck1.dependencyTablePartitionSize = 20;
        depCheck1.dependencyTablePartitionUnit = PartitionUnit.MINUTES;
        assertFalse(PartitionDurationTester.checkDependency(tableDur1, depCheck1));

        var tableDur2 = Duration.ofMinutes(40);
        assertTrue(PartitionDurationTester.checkDependency(tableDur2, depCheck1));

        var tableDur3 = Duration.ofHours(6);
        var depCheck3 = new PartitionDurationTester.PartitionTestDependencyInput();
        depCheck3.dependencyTableName = "check3";
        depCheck3.transformType = PartitionTransformType.AGGREGATE;
        depCheck3.transformSize = 24;
        depCheck3.dependencyTablePartitionSize = 15;
        depCheck3.dependencyTablePartitionUnit = PartitionUnit.MINUTES;
        assertTrue(PartitionDurationTester.checkDependency(tableDur3, depCheck3));
    }

    @Test
    void fullDependencyCheck() {
        // Does a full test of three dependencies and a target table with different types of transform
        PartitionUnit outTablePU = PartitionUnit.HOURS;
        int outTablePS = 1;
        var depA = new PartitionDurationTester.PartitionTestDependencyInput();
        depA.dependencyTableName = "depA";
        depA.transformType = PartitionTransformType.IDENTITY;
        depA.dependencyTablePartitionSize = 60;
        depA.dependencyTablePartitionUnit = PartitionUnit.MINUTES;
        var depB = new PartitionDurationTester.PartitionTestDependencyInput();
        depB.dependencyTableName = "depB";
        depB.transformType = PartitionTransformType.WINDOW;
        depB.transformSize = 6;
        depB.dependencyTablePartitionSize = 1;
        depB.dependencyTablePartitionUnit = PartitionUnit.HOURS;
        var depC = new PartitionDurationTester.PartitionTestDependencyInput();
        depC.dependencyTableName = "depC";
        depC.transformType = PartitionTransformType.AGGREGATE;
        depC.transformSize = 4;
        depC.dependencyTablePartitionSize = 15;
        depC.dependencyTablePartitionUnit = PartitionUnit.MINUTES;
        var deps = new PartitionDurationTester.PartitionTestDependencyInput[]{depA, depB, depC};
        var res = PartitionDurationTester.checkDependencies(
                outTablePU, outTablePS, deps);
        assertTrue(res.success);

        // If we change to 3*15 minutes it should not work anymore
        depC.transformSize = 3;
        var res2 = PartitionDurationTester.checkDependencies(
                outTablePU, outTablePS, deps);
        assertFalse(res2.success);
        assertEquals("depC", res2.badTableNames[0]);
    }
}
