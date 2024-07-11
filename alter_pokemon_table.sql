-- Añadir nuevas columnas
ALTER TABLE pokemon 
ADD COLUMN IF NOT EXISTS random_id BIGINT;

-- Crea índice único para random_id
CREATE UNIQUE INDEX IF NOT EXISTS idx_pokemon_random_id ON pokemon(random_id);

-- Crear función para generar IDs aleatorios
CREATE OR REPLACE FUNCTION generate_random_id() RETURNS BIGINT AS $$
DECLARE
    new_id BIGINT;
    done BOOL;
BEGIN
    done := FALSE;
    WHILE NOT done LOOP
        new_id := (random() * 9223372036854775807)::BIGINT;
        done := NOT EXISTS (SELECT 1 FROM pokemon WHERE random_id = new_id);
    END LOOP;
    RETURN new_id;
END;
$$ LANGUAGE plpgsql VOLATILE;

-- Poblar random_id para registros existentes
UPDATE pokemon
SET random_id = generate_random_id()
WHERE random_id IS NULL;

-- Asegurarse de que random_id no sea nulo en el futuro
ALTER TABLE pokemon
ALTER COLUMN random_id SET NOT NULL;
