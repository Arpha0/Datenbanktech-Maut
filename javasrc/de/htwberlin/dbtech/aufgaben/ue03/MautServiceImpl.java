package de.htwberlin.dbtech.aufgaben.ue03;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
		boolean istManuellRegistriert = istManuellRegistriert(mautAbschnitt, kennzeichen);
		// TODO weitere Logik
		if(!istAutoRegistriert && !istManuellRegistriert) {
			throw new UnkownVehicleException("Fahrzeug nicht registriert");
		}

		if (istAutoRegistriert) {
			int gespeicherteAchsen = getAchsenFuerFahrzeug(kennzeichen);

			if (gespeicherteAchsen != achszahl) {
				throw new InvalidVehicleDataException("Achszahl stimmt nicht überein");
			}
		}

	}

	private int getAchsenFuerFahrzeug(String kennzeichen) {
		String sql = "SELECT Achsen FROM Fahrzeug WHERE Kennzeichen = ? AND Abmeldedatum IS NULL";

		try (PreparedStatement s = connection.prepareStatement(sql)) {
			s.setString(1, kennzeichen);

			try (ResultSet rs = s.executeQuery()) {
				if (rs.next()) {
					return rs.getInt("Achsen");
				} else {
					// Dieser Fall sollte eigentlich nicht eintreten, da wir vorher 'istAutoRegistriert' prüfen,
					// aber sicher ist sicher.
					throw new DataException("Fahrzeug mit Kennzeichen " + kennzeichen + " nicht gefunden.");
				}
			}
		} catch (SQLException e) {
			throw new DataException(e);
		}
	}




	private boolean istManuellRegistriert(int mautAbschnitt, String kennzeichen) {
		try (PreparedStatement s = connection.prepareStatement("SELECT * FROM Buchung b" +
				" WHERE Kennzeichen = ? AND Abschnitts_Id = ?")) {
			s.setString(1, kennzeichen);
			s.setInt(2, mautAbschnitt);
			ResultSet rs = s.executeQuery();
			return rs.next();
		} catch (SQLException e){
			throw new DataException(e);
		}
	}

	// Methode prüft, ob Fahrzeug im automatischen Verfahren bekannt ist
	private boolean istAutoRegistriert(String kennzeichen) {
		boolean istAutoRegistriert = false;
		String sql = "SELECT * " +
				"FROM Fahrzeug " +
				"WHERE Kennzeichen = ?" +
				"AND Abmeldedatum IS NULL";
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
