package com.dmslob;

import com.dmslob.entity.Contact;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceConfiguration;
import jakarta.persistence.TypedQuery;
import org.testng.annotations.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

// https://jakarta.ee/specifications/persistence/3.2/
public class PersistenceConfigurationTest {

    EntityManagerFactory jpaEntityManagerFactory() {
        return new PersistenceConfiguration("postgres")
                .property("jakarta.persistence.jdbc.url", "jdbc:postgresql://localhost:5432/postgres")
                .property("jakarta.persistence.jdbc.driver", "org.postgresql.Driver")
                .property("jakarta.persistence.jdbc.user", "postgres")
                .property("jakarta.persistence.jdbc.password", "MilkyWay80")
                .managedClass(Contact.class)
                .property(PersistenceConfiguration.LOCK_TIMEOUT, 5000)
                .createEntityManagerFactory();
    }

    @Test
    public void should_find_contact() {
        // given
        try (var emf = jpaEntityManagerFactory()) {
            // when
            var contact = emf.callInTransaction(em -> em.find(Contact.class, 8));
            // then
            assertThat(contact).isNotNull();
            assertThat(contact.getId()).isEqualTo(8);
        }
    }

    @Test
    public void should_test_streamlined_jpql() {
        // given
        long expectedId = 10;
        try (var emf = jpaEntityManagerFactory()) {
            // when
            Contact contact = emf.callInTransaction(em ->
                    em.createQuery("from Contact where name = 'Jack Doe'",
                            Contact.class).getSingleResult()
            );
            // then
            assertThat(contact).isNotNull();
            assertThat(contact.getId()).isEqualTo(expectedId);
        }
    }

    @Test
    public void should_test_cast_function() {
        // given
        Integer expected = 987654;
        try (var emf = jpaEntityManagerFactory()) {
            // when
            var integer = emf.callInTransaction(em -> {
                TypedQuery<Integer> query = em.createQuery(
                        "select cast(c.phoneNumber as integer) from Contact c where c.id = 8", Integer.class
                );
                return query.getSingleResult();
            });
            // then
            assertThat(integer).isNotNull();
            assertThat(integer).isEqualTo(expected);
        }
    }

    @Test
    public void should_test_left_right_replace_functions() {
        // given
        String expectedLeftName = "Jane";
        try (var emf = jpaEntityManagerFactory()) {
            // when use left function
            var leftName = emf.callInTransaction(em -> {
                TypedQuery<String> query = em.createQuery(
                        "select left(c.name, 4) from Contact c where c.id = 8", String.class
                );
                return query.getSingleResult();
            });
            // then
            assertThat(leftName).isNotNull();
            assertThat(leftName).isEqualTo(expectedLeftName);
            // when use right function
            String expectedRightName = "Doe";
            // when
            var rightName = emf.callInTransaction(em -> {
                TypedQuery<String> query = em.createQuery(
                        "select right(c.name, 3) from Contact c where c.id = 8", String.class
                );
                return query.getSingleResult();
            });
            // then
            assertThat(rightName).isNotNull();
            assertThat(rightName).isEqualTo(expectedRightName);
            // when use replace function
            String expectedName = "Janny Doe";
            // when
            var replacedName = emf.callInTransaction(em -> {
                TypedQuery<String> query = em.createQuery(
                        "select replace(c.name, 'Jane', 'Janny') from Contact c where c.id = 8", String.class
                );
                return query.getSingleResult();
            });
            // then
            assertThat(replacedName).isNotNull();
            assertThat(replacedName).isEqualTo(expectedName);
        }
    }

    // id() method lets us extract the identifier of the database record:
    @Test
    public void should_test_id_function() {
        // given
        Long expected = 8L;
        try (var emf = jpaEntityManagerFactory()) {
            // when
            var id = emf.callInTransaction(em -> {
                TypedQuery<Long> query = em.createQuery(
                        "select id(c) from Contact c where c.phoneNumber='987654'", Long.class
                );
                return query.getSingleResult();
            });
            // then
            assertThat(id).isNotNull();
            assertThat(id).isEqualTo(expected);
        }
    }

    @Test
    public void should_insert_contact() {
        // given
        try (var emf = jpaEntityManagerFactory()) {
            // when
            //emf.runInTransaction(em -> em.persist(new Contact("alice", "111111")));
            //emf.runInTransaction(em -> em.persist(new Contact("Bob McDonald's", "222222")));
            //emf.runInTransaction(em -> em.persist(new Contact("charley", "333333")));
            //emf.runInTransaction(em -> em.persist(new Contact(null, "333333")));
            // then
            List<Contact> contacts =  emf.callInTransaction(em -> {
                TypedQuery<Contact> query = em.createQuery(
                        "SELECT c FROM Contact c ORDER BY lower(c.name) ASC NULLS FIRST",
                        Contact.class);
                return query.getResultList();
            });
            assertThat(contacts.get(0).getName()).isNull();
            assertThat(contacts.get(1).getName()).isEqualTo("alice");
            assertThat(contacts.get(2).getName()).isEqualTo("Bob McDonald's");
            assertThat(contacts.get(3).getName()).isEqualTo("charley");
        }
    }
}
