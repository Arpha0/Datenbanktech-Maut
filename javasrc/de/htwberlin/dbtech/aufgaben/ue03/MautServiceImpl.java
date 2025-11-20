package de.htwberlin.dbtech.aufgaben.ue03;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.dbtech.exceptions.DataException;
import de.htwberlin.dbtech.exceptions.AlreadyCruisedException;
import de.htwberlin.dbtech.exceptions.InvalidVehicleDataException;
import de.htwberlin.dbtech.exceptions.UnkownVehicleException;

/**
 * Die Klasse realisiert den AusleiheService.
 * 
 * @author Patrick Dohmeier
 */
public class MautServiceImpl implements IMautService {

	private static final Logger L = LoggerFactory.getLogger(MautServiceImpl.class);
	private Connection connection;

	@Override
	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	private Connection getConnection() {
		if (connection == null) {
			throw new DataException("Connection not set");
		}
		return connection;
	}

	@Override
	public void berechneMaut(int mautAbschnitt, int achszahl, String kennzeichen)
			throws UnkownVehicleException, InvalidVehicleDataException, AlreadyCruisedException {
		// 1. ist das Fahrzeug bekannt?
		boolean istAutoRegistriert = istAutoRegistriert(kennzeichen);
		// TODO weitere Logik
	}

	// Methode prüft, ob Fahrzeug im automatischen Verfahren bekannt ist
	private boolean istAutoRegistriert(String kennzeichen) {
		boolean istAutoRegistriert = false;
		String sql = "SELECT * " +
				"FROM Fahrzeug f" +
				"WHERE Kennzeichen = ?" +
				"AND f.Abmeldedatum is NULL";
		try (PreparedStatement s = connection.prepareStatement(sql)) {
			s.setString(1, kennzeichen);
			ResultSet rs = s.executeQuery();
			istAutoRegistriert = rs.next();
		}
		catch (Exception e) {
			throw new DataException(e);
		}
		return istAutoRegistriert;
	}



}
