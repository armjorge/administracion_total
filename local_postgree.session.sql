-- Add missing columns to the table
ALTER TABLE banorte_conceptos.clasificadores ADD COLUMN beneficiario VARCHAR(255);
ALTER TABLE banorte_conceptos.clasificadores ADD COLUMN grupo VARCHAR(255);
ALTER TABLE banorte_conceptos.clasificadores ADD COLUMN categoria VARCHAR(255);
ALTER TABLE banorte_conceptos.clasificadores ADD COLUMN concepto VARCHAR(255);

-- Insert categoria values
INSERT INTO banorte_conceptos.clasificadores (categoria) VALUES
('Pensiones'),
('Reembolsos Eseotres'),
('Reembolsos Bombón'),
('Casa'),
('Ocio'),
('Salud');

-- Insert grupo values
INSERT INTO banorte_conceptos.clasificadores (grupo) VALUES
('Gasto fijo'),
('No clasificado'),
('Vivienda');